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
    unsigned id, const string &addr,
    const filesystem::path &dax_path)
{
    boost::property_tree::ptree config;
    {
        ifstream f(config_path);
        boost::property_tree::read_ini(f, config);
    }

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

    /* get mapped DAX */
    void *pmem_space;
    size_t pmem_size;
    {
        if (!filesystem::is_character_file(dax_path)) {
            ostringstream what;
            what << "Cannot map DEVDAX at " << dax_path;
            BOOST_LOG_TRIVIAL(fatal) << what.str();
            throw std::runtime_error(what.str());
        }
        pmem_space = pmem_map_file(
            dax_path.c_str(), /*length=entire file*/0, /*flag*/0, /*mode*/0,
            &pmem_size, NULL);
        if (!pmem_space) {
            ostringstream what;
            what << "Failed to map DEVDAX at " << dax_path;
            BOOST_LOG_TRIVIAL(fatal) << what.str();
            throw std::runtime_error(what.str());
        }
    }
    decltype(Server::pmem_space) managed_pmem_map(pmem_space,
        [=](void*) { pmem_unmap(pmem_space, pmem_size); });

    /* get RNIC */
    string rnic_name;
    {
        int num_rnics;
        auto rnics = rdma_get_devices(&num_rnics);
        defer([&] { rdma_free_devices(rnics); });
        if (!num_rnics) {
            BOOST_LOG_TRIVIAL(fatal) << "No RNIC found";
            throw std::runtime_error("No RNIC");
        }
        auto chosen = gestalt::misc::numa::choose_rnic_on_same_numa(
            dax_path.c_str(), rnics, num_rnics);
        if (!chosen) {
            BOOST_LOG_TRIVIAL(warning) << "Cannot find a matching RNIC on the "
                << "same NUMA as the DEVDAX, using the first RNIC listed "
                << "instead!";
            chosen = rnics[0]->device;
        }
        rnic_name = chosen->name;
    }

    /* register memory region */

    return make_unique<Server>(
        id, config,
        std::move(managed_pmem_map), pmem_space,
        boost::asio::ip::make_address(addr)
    );
}

Server::~Server()
{
    // TODO:
}

}   /* namespace gestalt */
