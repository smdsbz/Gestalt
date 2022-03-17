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
    bool ycsb_regen = false;
    string log_level;
    unsigned client_id = 114514;

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
    ycsb_load.reserve(1e5);
    ycsb_run.reserve(2e6);
    {
        namespace yp = smdsbz::ycsb_parser;
        const auto args_path = cur_src_dir / "ycsb_args.tmp";

        /**
         * CMBK: Tune #ordered_args in source, the program will automatically
         * re-run YCSB if it detects a matching workload has not been generated
         * (detect by checking dumped args).
         */
        vector<pair<string, string>> ordered_args{
            {"workload", (filesystem::path(YCSB_WORKLOAD_DIR) / "workloada").string()},
            {"recordcount", to_string(static_cast<int>(1e5))},
            {"operationcount", to_string(static_cast<int>(2e6))},
            // {"requestdistribution", "uniform"},
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

    /* notify followers to read load */

    gestalt::Client coord_client(config_path, client_id);
    /* wait a while for YCSB trace update to be published via NFS */
    const auto load_ready_at = std::chrono::system_clock::now() + 2s;
    {
        const auto ts =
            std::chrono::duration_cast<std::chrono::microseconds>(
                load_ready_at.time_since_epoch())
            .count();
        if (int r = coord_client.put("load_ready_at", &ts, sizeof(ts)); r) {
            errno = -r;
            boost_log_errno_throw(coord_client.put);
        }
        BOOST_LOG_TRIVIAL(info) << "load_ready_at " << ts;
    }

    // uint64_t completed_ops = 0;
    // if (int r = coord_client.put("completed_ops", &completed_ops, sizeof(completed_ops)); r) {
    //     errno = -r;
    //     boost_log_errno_throw(coord_client.put completed_ops);
    // }
    // uint64_t num_client_nodes = 0;
    // // if (int r = coord_client.put("num_client_nodes", &completed_ops, sizeof(completed_ops)); r) {
    // //     errno = -r;
    // //     boost_log_errno_throw(coord_client.put completed_ops);
    // }

    /* load */

    while (std::chrono::system_clock::now() < load_ready_at)
        [[unlikely]] ;

    /** @note insert collisions will be ignored */
    size_t successful_insertions = 0;
    {
        gestalt::Client client(config_path, client_id);
        BOOST_LOG_TRIVIAL(info) << "Loading workload into Gestalt ...";
        for (const auto &d : ycsb_load) {
            uint8_t buf[4_K];
            /* HACK: we don't fill with actual data!
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

    /* get set */

    /* wait for followers to download trace, setup per-thread trace and
        initialize client connection */
    const auto start_at = std::chrono::system_clock::now() + 35s;
    /* run for fixed duration, calculate metrics by dividing with reported completed operations */
    const auto test_duration = 20s;
    const auto end_at = start_at + test_duration;
    {
        const auto start_ts =
            std::chrono::duration_cast<std::chrono::microseconds>(
                start_at.time_since_epoch())
            .count();
        const auto end_ts =
            std::chrono::duration_cast<std::chrono::microseconds>(
                end_at.time_since_epoch())
            .count();
        if (int r = coord_client.put("start_at", &start_ts, sizeof(start_ts)); r) {
            errno = -r;
            boost_log_errno_throw(coord_client.put start_at);
        }
        if (int r = coord_client.put("end_at", &end_ts, sizeof(end_ts)); r) {
            errno = -r;
            boost_log_errno_throw(coord_client.put end_at);
        }
        BOOST_LOG_TRIVIAL(info) << "start_ts " << start_ts
            << ", end_ts " << end_ts;
    }

    while (std::chrono::system_clock::now() < end_at)
        [[unlikely]] ;
    BOOST_LOG_TRIVIAL(info) << "Test duration passed, we should stop";

    // /* wait for completed ops to be updated */
    // std::this_thread::sleep_for(2s);

    // if (int r = coord_client.get("completed_ops"); r) {
    //     errno = -r;
    //     boost_log_errno_throw(coord_client.get completed_ops);
    // }
    // completed_ops = *reinterpret_cast<uint64_t*>(coord_client.read_op->buf.data());
    // BOOST_LOG_TRIVIAL(info) << "completed_ops " << completed_ops;
    // if (int r = coord_client.get("num_client_nodes"); r) {
    //     errno = -r;
    //     boost_log_errno_throw(coord_client.get num_client_nodes);
    // }
    // num_client_nodes = *reinterpret_cast<uint64_t*>(coord_client.read_op->buf.data());
    // BOOST_LOG_TRIVIAL(info) << "num_client_nodes " << num_client_nodes;

    // /* calculate metrics */
    // BOOST_LOG_TRIVIAL(info) << std::left << std::fixed;
    // BOOST_LOG_TRIVIAL(info)
    //     << std::setw(16) << "avg lat (us)"
    //     << std::setw(16) << "Miops"
    //     << std::setw(16) << "bw (GiB/s)";
    // {
    //     const double lat_us = 1e6 * test_duration.count() / (1. * completed_ops / num_client_nodes);
    //     const double miops = 1. * completed_ops / 1e6 / test_duration.count();
    //     const double bw_GiB = completed_ops * 4_K * 1. / 1_G / test_duration.count();
    //     BOOST_LOG_TRIVIAL(info)
    //         << std::setw(16) << lat_us
    //         << std::setw(16) << miops
    //         << std::setw(16) << bw_GiB;
    // }
    // BOOST_LOG_TRIVIAL(info) << std::right;

    return EXIT_SUCCESS;
}
