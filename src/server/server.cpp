/**
 * @file server.cpp
 *
 * Implementation of the server instance.
 */

#include <sstream>
#include <fstream>

#include <libpmem.h>
#include <rdma/rdma_cma.h>
#include <boost/log/trivial.hpp>
#include <boost/property_tree/ini_parser.hpp>

#include <grpcpp/channel.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/client_context.h>
#include "ClusterMap.pb.h"
#include "ClusterMap.grpc.pb.h"

#include "./server.hpp"
#include "misc/numa.hpp"
#include "misc/ddio.hpp"
#include "common/defer.hpp"


namespace gestalt {
using namespace std;

unique_ptr<Server> Server::create(
    const filesystem::path &config_path,
    unsigned id, const string &addr)
{
    boost::property_tree::ptree config;
    {
        ifstream f(config_path);
        boost::property_tree::read_ini(f, config);
    }

    /* TODO: resolve IP, and get RNIC interface */


    /* add self to cluster map, retrieve server ID */
    {
        using namespace grpc;
        using namespace gestalt::rpc;

        auto mon_chan = CreateChannel(
            config.get_child("global.monitor_address").get_value<string>(),
            InsecureChannelCredentials());
        auto mon_stub = ClusterMap::NewStub(mon_chan);

        ClientContext ctx;
        ServerProp in, out;
        in.set_id(id);
        in.set_addr(addr);
        if (auto r = mon_stub->AddServer(&ctx, in, &out); !r.ok()) {
            ostringstream what;
            what << "Failed to add self to cluster map, monitor complained: "
                << r.error_message();
            BOOST_LOG_TRIVIAL(error) << what.str();
            throw std::runtime_error(what.str());
        }

        id = out.id();
    }
    BOOST_LOG_TRIVIAL(info) << "Successfully joined cluster map, with ID " << id;

    /* TODO: map devdax space */


    return make_unique<Server>(
        id, config,
        nullptr, boost::asio::ip::make_address(addr)
    );
}

Server::~Server()
{
    // TODO:
}

}   /* namespace gestalt */
