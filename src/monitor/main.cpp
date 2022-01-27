/**
 * @file
 *
 * Gestalt cluster monitor
 *
 * Currently implemented as a service consisting of only one instance.
 */

#include <mutex>
#include <map>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <sstream>

#include <boost/log/trivial.hpp>
#include "common/boost_log_helper.hpp"
#include <boost/program_options.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/asio/ip/address.hpp>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/security/server_credentials.h>
#include "ClusterMap.pb.h"
#include "ClusterMap.grpc.pb.h"

#include "defaults.hpp"

using namespace std;


namespace gestalt {
namespace rpc {

using namespace grpc;
using namespace google::protobuf;


class ClusterMapServicer final : public gestalt::rpc::ClusterMap::Service {

    mutable std::mutex _mutex;

    struct server_prop_t {
        boost::asio::ip::address addr;
    public:
        server_prop_t(const boost::asio::ip::address &_addr) noexcept :
            addr(_addr)
        { }
    };
    map<unsigned, server_prop_t> server_props;  ///< server ID -> properties

public:
    ClusterMapServicer() : server_props()
    { }

    Status AddServer(ServerContext *ctx,
        const ServerProp *in, ServerProp *out) override
    {
        std::scoped_lock l(_mutex);

        BOOST_LOG_TRIVIAL(info) << "AddServer request from peer " << ctx->peer();

        unsigned new_id;
        /* forcing an ID */
        if (new_id = in->id(); new_id) {
            if (server_props.contains(new_id)) {
                BOOST_LOG_TRIVIAL(warning) << "Try re-registering a server ID "
                    << new_id << ", do nothing";
                return Status(StatusCode::ALREADY_EXISTS,
                    "server with this ID already exists");
            }
        }
        /* generate new ID */
        else {
            const auto &last = server_props.rbegin();
            new_id = (last == server_props.rend()) ? 1 : last->first + 1;
        }

        /* verifying address */
        boost::asio::ip::address addr;
        try {
            addr = boost::asio::ip::make_address(in->addr());
        }
        catch (std::exception &e) {
            BOOST_LOG_TRIVIAL(warning) << "Failed to digest server address "
                << in->addr() << ": " << e.what();
            return Status(StatusCode::INVALID_ARGUMENT, "addr");
        }

        server_props.insert({new_id, {addr}});
        BOOST_LOG_TRIVIAL(info) << "Registered server " << new_id
            << " @ " << in->addr();

        out->set_id(new_id);
        return Status::OK;
    }

    Status GetServers(ServerContext *ctx,
        const Empty *in, ServerList *out) override
    {
        std::scoped_lock l(_mutex);
        for (const auto &[id, prop] : server_props) {
            auto p = out->add_servers();
            p->set_id(id);
            ostringstream addr;
            addr << prop.addr;
            p->set_addr(addr.str());
        }
        return Status::OK;
    }
};  /* class ClusterMapServicer */

}   /* namespace rpc */
}   /* namespace gestalt */


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

    set_boost_log_level(log_level);

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
    boost::property_tree::ptree config;
    {
        ifstream f(config_path);
        boost::property_tree::read_ini(f, config);
    }

    /* start gRPC service */
    gestalt::rpc::ClusterMapServicer clustermap_svc;
    grpc::ServerBuilder grpc_builder;
    const auto &clustermap_server_port =
        config.get_child("global.monitor_address").get_value<string>();
    grpc_builder.AddListeningPort(clustermap_server_port, grpc::InsecureServerCredentials());
    grpc_builder.RegisterService(&clustermap_svc);
    auto clustermap_grpc_server = grpc_builder.BuildAndStart();
    BOOST_LOG_TRIVIAL(info) << "ClusterMap service started on " << clustermap_server_port;

    clustermap_grpc_server->Wait();

    return 0;
}
