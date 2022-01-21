/**
 * @file server.cpp
 *
 * Implementation of the server instance.
 */

#include <fstream>

#include <libpmem.h>
#include <rdma/rdma_cma.h>
#include <boost/log/trivial.hpp>
#include "common/boost_log_helper.hpp"
#include <boost/property_tree/ini_parser.hpp>

#include <grpcpp/channel.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include "ClusterMap.pb.h"
#include "ClusterMap.grpc.pb.h"

#include "server.hpp"
#include "misc/numa.hpp"
#include "misc/ddio.hpp"
#include "common/defer.hpp"


namespace gestalt {
using namespace std;

unique_ptr<Server> Server::create(const filesystem::path &config_path)
{
    boost::property_tree::ptree config;
    {
        ifstream f(config_path);
        boost::property_tree::read_ini(f, config);
    }

    /* determine server ID, and add self to cluster map */
    auto grpc_chan = grpc::CreateChannel(
        config.get_child("config.monitor_address").get_value<string>(),
        grpc::InsecureChannelCredentials());

    /* map devdax space */

    return nullptr;
}

}   /* namespace gestalt */
