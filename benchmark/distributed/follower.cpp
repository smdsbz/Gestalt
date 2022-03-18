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
#include <numeric>

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

    gestalt::Client coord_client(config_path, client_id + 1919810);

    {
        int64_t ts = 0;
        while (!ts) {
            if (int r = coord_client.get("load_ready_at"); r)
                [[unlikely]] continue;
            ts = *reinterpret_cast<int64_t*>(coord_client.read_op->buf.data());
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
    constexpr unsigned thread_nr_to_test = 16;
    BOOST_LOG_TRIVIAL(info) << "Generating trace for each thread (total "
        << thread_nr_to_test << " threads for this client instance) ...";
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

    bool start_flag = false, stop_flag = false;

    vector<unsigned long long> thread_completed_ops(thread_nr_to_test, 0);
    const auto thread_test_fn = [&] (const unsigned thread_id) {
        auto &completed_ops = thread_completed_ops.at(thread_id);
        gestalt::Client client(config_path, client_id * 1000 + thread_id);

        while (!start_flag)
            [[unlikely]] ;

        for (const auto &d : thread_run[thread_id]) {
            using Op = decltype(d.op);
            if (stop_flag)
                break;

            bool retry = true;
            while (retry && !stop_flag) {
                retry = false;
                switch (d.op) {
                case Op::READ: {
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
                    else
                        completed_ops++;
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
                    else
                        completed_ops++;
                    break;
                }
                default:
                    throw std::runtime_error("unexpected run op");
                }
            }
        }

        if (!stop_flag)
            throw std::runtime_error("trace ended prematurely");
    };

    vector<std::jthread> test_thread_pool;
    for (unsigned i = 0; i < thread_nr_to_test; i++)
        test_thread_pool.push_back(std::jthread(thread_test_fn, i));

    {
        int64_t start_ts, end_ts;
        if (int r = coord_client.get("start_at"); r) {
            errno = -r;
            boost_log_errno_throw(coord_client.get start_at);
        }
        start_ts = *reinterpret_cast<int64_t*>(coord_client.read_op->buf.data());
        BOOST_LOG_TRIVIAL(info) << "start_at " << start_ts;
        if (int r = coord_client.get("end_at"); r) {
            errno = -r;
            boost_log_errno_throw(coord_client.get end_at);
        }
        end_ts = *reinterpret_cast<int64_t*>(coord_client.read_op->buf.data());
        BOOST_LOG_TRIVIAL(info) << "end_ts " << end_ts;

        bool has_waited = false;
        while (std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                .count() < start_ts)
            [[unlikely]] has_waited = true;
        start_flag = true;
        if (!has_waited) {
            stop_flag = true;
            throw std::runtime_error("thread initialization took to long");
        }
        BOOST_LOG_TRIVIAL(info) << "Test started";

        while (std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                .count() < end_ts)
            [[unlikely]];
        stop_flag = true;
        BOOST_LOG_TRIVIAL(info) << "Test should now be terminated";
    }

    std::this_thread::sleep_for(2s);

    BOOST_LOG_TRIVIAL(info) << "total_completed_ops "
        << std::accumulate(thread_completed_ops.begin(), thread_completed_ops.end(), 0ull);


    return EXIT_SUCCESS;
}
