/**
 * @file
 *
 * Server process
 */

#include <filesystem>

#include <boost/log/trivial.hpp>
#include "common/boost_log_helper.hpp"
#include <boost/program_options.hpp>

#include "defaults.hpp"
#include "./server.hpp"

using namespace std;


int main(const int argc, const char **argv)
{
    /* process arguments */

    filesystem::path config_path;   // path to config file
    string log_level;               // Boost log level
    unsigned server_id = 0;         // specified server ID
    string server_addr;             // specified server address
    filesystem::path dax_path;      // path to devdax

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
            ("id", po::value(&server_id), "specify server ID")
            ("addr", po::value(&server_addr), "specify server address")
            ("dax-dev", po::value(&dax_path)->required(),
                "Path to DEVDAX device.")
            ;
        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);
    }

    set_boost_log_level(log_level);

    /* test config file */
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


    /* TODO: run server */

    auto server_runtime = gestalt::Server::create(
        config_path, server_id, server_addr, dax_path);
    BOOST_LOG_TRIVIAL(info) << "Server runtime successfully created!";

    /* TODO: register stop() to SIGINT */

    server_runtime->run();

    return 0;
}
