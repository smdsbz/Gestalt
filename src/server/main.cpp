/**
 * @file
 *
 * Server process
 */

#include <filesystem>
#include <vector>
#include <fstream>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/program_options.hpp>
#include "defaults.hpp"

using namespace std;


int main(const int argc, const char **argv)
{
    /* process arguments */

    filesystem::path config_path;   // path to config file
    string log_level;               // Boost log level

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
            ;
        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);
    }

    /* set Boost log level */
    {
        bool isset = false;
#define LOG_LEVEL_SET(lv)                                                   \
do {                                                                        \
    namespace logging = boost::log;                                         \
    if (!isset && log_level == #lv) {                                       \
        logging::core::get()->set_filter(                                   \
            logging::trivial::severity >= logging::trivial::lv              \
        );                                                                  \
        isset = true;                                                       \
    }                                                                       \
} while (0)
        LOG_LEVEL_SET(trace);
        LOG_LEVEL_SET(debug);
        LOG_LEVEL_SET(info);
        LOG_LEVEL_SET(warning);
        LOG_LEVEL_SET(error);
        LOG_LEVEL_SET(fatal);
        if (!isset)
            LOG_LEVEL_SET(info);
#undef LOG_LEVEL_SET
    }

    /* load config file */
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

    {
        // TODO: parse config file
    }


    return 0;
}
