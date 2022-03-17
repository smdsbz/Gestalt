/**
 * @file 
 *
 * Distributed client benchmark
 */

#include <filesystem>
#include <vector>
#include <unordered_map>
#include <map>
#include <random>
#include <fstream>
#include <sstream>
#include <memory>
#include <thread>
#include <atomic>
#include <chrono>
using namespace std::chrono_literals;

#include "common/boost_log_helper.hpp"
#include <boost/program_options.hpp>

#include "defaults.hpp"
#include "client.hpp"
#include "ycsb_parser.hpp"
#include "ycsb.h"

using namespace std;


int main(const int argc, const char **argv)
{
    filesystem::path src_dir = 
        filesystem::absolute(argv[0]).parent_path().parent_path().parent_path();
    filesystem::path cur_src_dir = src_dir / "benchmark" / "distributed";
    filesystem::path config_path;
    filesystem::path ycsb_load_path = src_dir / "workload" / "load.ycsb";
    filesystem::path ycsb_run_path = src_dir / "workload" / "run.ycsb";
    string log_level;
    unsigned client_id;

    {
        namespace po = boost::program_options;
        po::options_description desc;
        desc.add_options()
            ("config", po::value(&config_path),
                "Configuration file, if not given, will search for "
                "/etc/gestalt/gestalt.conf, ./gestalt.conf, "
                "./etc/gestalt/gestalt.conf, whichever comes first.")
            ("log", po::value(&log_level)->default_value("info"),
                "Logging level (Boost).")
            ("id", po::value(&client_id)->required(), "specify client ID")
            ("ycsb-load", po::value(&ycsb_load_path), "YCSB load output")
            ("ycsb-run", po::value(&ycsb_run_path), "YCSB run output")
            ;
        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);
    }

    set_boost_log_level(log_level);

    if (config_path.empty()) {
        for (auto &p : gestalt::defaults::config_paths) {
            if (!filesystem::is_regular_file(p))
                continue;
            config_path = p;
            break;
        }
    }
    if (!filesystem::is_regular_file(config_path)) {
        BOOST_LOG_TRIVIAL(fatal) << "Cannot find configuration file";
        exit(EXIT_FAILURE);
    }

    /* wait for run trace ready */

    gestalt::Client admin_client(config_path, client_id + 114514);

    {
        int64_t ts = 0;
        while (!ts) {
            if (int r = admin_client.get("load_ready_at"); r) {
                continue;
            }
            ts = *reinterpret_cast<int64_t*>(admin_client.read_op->buf.data());
        }
        BOOST_LOG_TRIVIAL(info) << "load_ready_at " << ts;
        while (std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                .count() < ts)
            [[unlikely]] ;
    }

    /* load run trace */

    smdsbz::ycsb_parser::trace ycsb_run;
    {
        ycsb_run.reserve(1e6);
        smdsbz::ycsb_parser::parse(ycsb_run_path, ycsb_run);
    }
    BOOST_LOG_TRIVIAL(info) << "YCSB workload loaded";

    /* generate scrambled YCSB run trace for each thread, minimizing CPU cache
        miss impact */
    constexpr unsigned thread_nr_to_test = 64;
    BOOST_LOG_TRIVIAL(info) << "Generating trace for each thread ...";
    vector<decltype(ycsb_run)> thread_run(thread_nr_to_test);
    {
        std::random_device rd;
        std::default_random_engine re(rd());
        std::uniform_int_distribution<unsigned> dist(0, ycsb_run.size() - 1);
        for (auto &tt : thread_run) {
            tt.reserve(1e6);
            for (unsigned i = 0; i < ycsb_run.size(); i++)
                tt.push_back(ycsb_run.at(dist(re)));
        }
    }
    BOOST_LOG_TRIVIAL(info) << "Thread-specific trace generated";

    std::atomic<bool> start_flag = false, stop_flag = false;

    const auto thread_test_fn = [&] (const unsigned thread_id, uint64_t &completed_ops) {
        gestalt::Client client(config_path, client_id + 200 + thread_id);

        while (!start_flag)
            [[unlikely]] ;

        for (const auto &d : thread_run[thread_id]) {
            using Op = decltype(d.op);
            bool retry = true;
            while (retry) {
                retry = false;
                switch (d.op) {
                case Op::READ: {
                    #if 1   // set to 0 will skip validity checking
                    if (int r = client.get(d.okey.c_str()); r) {
                        /* key temporarily locked */
                        [[unlikely]] if (r == -EAGAIN || r == -ECOMM) {
                            [[unlikely]] retry = true;
                            // total_retries++;
                            break;
                        }
                        /* key not inserted */
                        [[unlikely]] if (r == -EINVAL)
                            [[likely]] break;
                        BOOST_LOG_TRIVIAL(warning) << "failed to read " << d.okey
                            << " : " << std::strerror(-r);
                    }
                    #else
                    client.raw_read(d.okey.c_str());
                    #endif
                    break;
                }
                case Op::UPDATE: {
                    uint8_t buf[4_K];
                    std::strcpy(reinterpret_cast<char*>(buf), d.okey.c_str());
                    if (int r = client.put(d.okey.c_str(), buf, sizeof(buf)); r) {
                        /* key temporarily locked */
                        [[unlikely]] if (r == -EBUSY) {
                            [[unlikely]] retry = true;
                            // total_retries++;
                            break;
                        }
                        /* key not inserted */
                        [[unlikely]] if (r == -EDQUOT)
                            [[likely]] break;
                        BOOST_LOG_TRIVIAL(warning) << "failed to update " << d.okey
                            << " : " << std::strerror(-r);
                    }
                    break;
                }
                default:
                    throw std::runtime_error("unexpected run op");
                }
            }
        }

        thread_finished_count++;
    };

    map<unsigned, std::chrono::duration<double>> thread_test_metrics;
    for (const auto &tnr : thread_nr_to_test) {
        BOOST_LOG_TRIVIAL(info) << "Running test for " << tnr << "-threads";

        thread_start_flag = false;
        thread_ready_count = thread_finished_count = 0;
        // total_retries = 0;

        vector<std::jthread> pool;
        for (unsigned i = 0; i < tnr; i++)
            pool.push_back(std::jthread(thread_test_fn, i));
        while (thread_ready_count != tnr) ;

        BOOST_LOG_TRIVIAL(info) << "Starting test ...";
        const auto start = std::chrono::steady_clock::now();
        thread_start_flag = true;
        while (thread_finished_count != tnr) ;
        const auto end = std::chrono::steady_clock::now();

        thread_test_metrics[tnr] = end - start;
        BOOST_LOG_TRIVIAL(info) << "Finished test for " << tnr << "-threads, "
            << thread_test_metrics[tnr].count() << "s has passed";
        // BOOST_LOG_TRIVIAL(info) << "total retires " << total_retries.load();
    }


    BOOST_LOG_TRIVIAL(info) << std::left << std::fixed;
    BOOST_LOG_TRIVIAL(info)
        << std::setw(8) << "thrd"
        << std::setw(16) << "avg lat (us)"
        << std::setw(16) << "Miops"
        << std::setw(16) << "bw (GiB/s)";
    for (const auto &[tnr, dur] : thread_test_metrics) {
        const double lat_us = 1e6 * dur.count() / ycsb_run.size();
        const double miops = ycsb_run.size() * tnr / 1e6 / dur.count();
        const double bw_GiB = ycsb_run.size() * 4_K * tnr / 1_G / dur.count();
        BOOST_LOG_TRIVIAL(info)
            << std::setw(8) << tnr
            << std::setw(16) << lat_us
            << std::setw(16) << miops
            << std::setw(16) << bw_GiB;
    }
    BOOST_LOG_TRIVIAL(info) << std::right;

    return EXIT_SUCCESS;
}
