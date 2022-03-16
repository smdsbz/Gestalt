/**
 * @file 
 *
 * Benchmark for latency
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
    filesystem::path cur_src_dir = src_dir / "benchmark" / "latency";
    filesystem::path config_path;
    filesystem::path ycsb_load_path = src_dir / "workload" / "load.ycsb";
    filesystem::path ycsb_run_path = src_dir / "workload" / "run.ycsb";
    bool ycsb_regen = false;
    string log_level;
    unsigned client_id = 114;

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
            ("id", po::value(&client_id), "specify client ID")
            ("ycsb-load", po::value(&ycsb_load_path), "YCSB load output")
            ("ycsb-run", po::value(&ycsb_run_path), "YCSB run output")
            ("ycsb-regen", po::value(&ycsb_regen)->default_value(false),
                "Force regeneration of YCSB workload")
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

    if (!filesystem::is_regular_file(ycsb_load_path)
            || !filesystem::is_regular_file(ycsb_run_path))
        ycsb_regen = true;

    /* prepare YCSB data */
    smdsbz::ycsb_parser::trace ycsb_load, ycsb_run;
    ycsb_load.reserve(1e4);
    ycsb_run.reserve(1e7);
    {
        namespace yp = smdsbz::ycsb_parser;
        const auto args_path = cur_src_dir / "ycsb_args.tmp";

        /**
         * Tune #ordered_args in source, the program will automatically re-run
         * YCSB if it detects a matching workload has not been generated (detect
         * by checking dumped args).
         */
        vector<pair<string, string>> ordered_args{
            {"workload", (filesystem::path(YCSB_WORKLOAD_DIR) / "workloada").string()},
            {"recordcount", to_string(static_cast<int>(1e5))},
            {"operationcount", to_string(static_cast<int>(1e6))},
            {"readproportion", to_string(1)},
            {"updateproportion", to_string(0)},
        };
        ostringstream serialized_args;
        for (const auto &a : ordered_args)
            serialized_args << a.first << "=" << a.second << " ";

        /* check if need to regen */
        do {
            /* if no previous run yet */
            ycsb_regen |= !filesystem::is_regular_file(args_path);
            if (ycsb_regen)
                break;

            /* if arg changed */
            {
                char old_args[4_K];
                ifstream f(args_path);
                f.read(old_args, sizeof(old_args));
                if (serialized_args.str() != old_args) {
                    ycsb_regen = true;
                    break;
                }
            }
        } while (0);

        if (ycsb_regen) {
            BOOST_LOG_TRIVIAL(info) << "Regenerating YCSB workload ...";
            {
                ofstream f(args_path, ios::out | ios::trunc);
                f << serialized_args.str();
            }
            yp::ycsb_args args(ordered_args.begin(), ordered_args.end());
            yp::dump_load(YCSB_BIN, args, ycsb_load_path);
            yp::dump_run(YCSB_BIN, args, ycsb_run_path);
        }

        BOOST_LOG_TRIVIAL(info) << "Loading YCSB workload into memory ...";
        yp::parse(ycsb_load_path, ycsb_load);
        yp::parse(ycsb_run_path, ycsb_run);
    }
    BOOST_LOG_TRIVIAL(info) << "YCSB workload loaded";


    /* setup client */

    gestalt::Client client(config_path, client_id);
    BOOST_LOG_TRIVIAL(info) << "client successfully setup";

    /* load, and heat up client locator cache */
    /** @note insert collisions will be ignored */
    size_t successful_insertions = 0;
    {
        BOOST_LOG_TRIVIAL(info) << "Loading workload into Gestalt ...";
        for (const auto &d : ycsb_load) {
            uint8_t buf[4_K];
            /* TODO: fill with actual data
                but it does not affect performance, so it is actually okay not
                to bother, just do something and yisi-yisi :`) */
            std::strcpy(reinterpret_cast<char*>(buf), d.okey.c_str());
            // BOOST_LOG_TRIVIAL(trace) << "putting " << d.okey;
            int r = client.put(d.okey.c_str(), buf, sizeof(buf));
            if (!r) {
                successful_insertions++;
                continue;
            }
            if (r == -EDQUOT) {
                BOOST_LOG_TRIVIAL(trace) << "failed inserting key " << d.okey
                    << ", ignored";
                continue;
            }
            errno = -r;
            boost_log_errno_throw(Client::put);
        }
        BOOST_LOG_TRIVIAL(info) << "Finished loading workload, loaded "
            << successful_insertions << " / " << ycsb_load.size()
            << " (" << 100. * successful_insertions / ycsb_load.size() << "%)";
    }

    /* run, single threaded, for latency test */
#if 0
    double single_threaded_ycsb_a;
    {
        BOOST_LOG_TRIVIAL(info) << "Start running workload ...";
        const auto start = std::chrono::steady_clock::now();

        for (const auto &d : ycsb_run) {
            using Op = decltype(d.op);
            switch (d.op) {
            case Op::READ: {
                if (int r = client.get(d.okey.c_str()); r) {
                    /* key not inserted */
                    [[unlikely]] if (r == -EINVAL)
                        [[likely]] break;
                    BOOST_LOG_TRIVIAL(warning) << "failed to read " << d.okey
                        << " : " << std::strerror(-r);
                }
                break;
            }
            case Op::UPDATE: {
                uint8_t buf[4_K];
                std::strcpy(reinterpret_cast<char*>(buf), d.okey.c_str());
                if (int r = client.put(d.okey.c_str(), buf, sizeof(buf)); r) {
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

        const auto end = std::chrono::steady_clock::now();
        const std::chrono::duration<double> interval = end - start;
        single_threaded_ycsb_a = interval.count() * 1e6 / ycsb_run.size();
        BOOST_LOG_TRIVIAL(info) << "Finished in " << interval.count() << " s, "
            << "avg lat " << single_threaded_ycsb_a << " us, "
            << "approx effective avg lat " << single_threaded_ycsb_a * ycsb_load.size() / successful_insertions << " us";
    }
#endif

    /* run, multi-threaded, for bandwidth (single-threaded falls back to latency test) */

    /* generate scrambled YCSB run trace for each thread, minimizing CPU cache
        miss impact */
    const vector<unsigned> thread_nr_to_test{1, 4, 16, 64}; // must be in asc order
    BOOST_LOG_TRIVIAL(info) << "Generating trace for each thread ...";
    vector<decltype(ycsb_run)> thread_run(thread_nr_to_test.back());
    {
        std::random_device rd;
        std::default_random_engine re(rd());
        std::uniform_int_distribution<unsigned> dist(0, ycsb_run.size() - 1);
        for (auto &tt : thread_run) {
            for (unsigned i = 0; i < ycsb_run.size(); i++)
                tt.push_back(ycsb_run.at(dist(re)));
        }
    }
    BOOST_LOG_TRIVIAL(info) << "Thread-specific trace generated";

    std::atomic<bool> thread_start_flag;
    std::atomic<unsigned> thread_ready_count, thread_finished_count;

    const auto thread_test_fn = [&] (const unsigned thread_id) {
        gestalt::Client client(config_path, client_id + 200 + thread_id);
        thread_ready_count++;
        while (!thread_start_flag)
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
