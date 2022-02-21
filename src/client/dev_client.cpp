/**
 * @file dev_client.cpp
 *
 * used for debugging only
 */

#include <filesystem>
#include <thread>

#include "common/boost_log_helper.hpp"
#include <boost/program_options.hpp>

#include "defaults.hpp"
#include "client.hpp"

using namespace std;


int main(const int argc, const char **argv)
{
    /* process arguments */
    filesystem::path config_path;
    string log_level;
    unsigned client_id = 0;     // global unique

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

    /* setup client */
    gestalt::Client client(config_path);
    BOOST_LOG_TRIVIAL(info) << "client successfully setup";

    /* test basic IO */
    char testbuf[] = "逸一时，误一世！";
    const char testkey[] = "yjsp";

    {
        if (int r = client.put(testkey, testbuf, sizeof(testbuf)); r) {
            errno = -r;
            boost_log_errno_throw(Client::put);
        }

        if (int r = client.get(testkey); r) {
            errno = -r;
            boost_log_errno_throw(Client::get);
        }

        {
            auto &r = client.read_op->buf;
            const auto actual = reinterpret_cast<const char*>(r.data()->data.get());
            BOOST_LOG_TRIVIAL(info) << "got out this: "
                << actual << ", expecting this: " << testbuf
                << std::endl;
            if (!strcmp(actual, testbuf))
                BOOST_LOG_TRIVIAL(info) << "good news! they do match!";
            else
                BOOST_LOG_TRIVIAL(error) << "BAD NEWS! they does not match!";
        }
    }

    return 0;
}
