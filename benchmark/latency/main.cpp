/**
 * @file 
 *
 * Benchmark for latency
 */

#include <filesystem>
#include <vector>
#include <fstream>
#include <sstream>
#include <memory>
#include <thread>
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
            {"operationcount", to_string(static_cast<int>(1e7))},
            // {"readproportion", to_string(0)},
            // {"updateproportion", to_string(1)},
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

    /* run, single threaded */
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

    /* run, multi-threaded */
    // TODO:

    return EXIT_SUCCESS;
}
