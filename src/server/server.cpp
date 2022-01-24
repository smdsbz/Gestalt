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
    managed_pmem_t managed_pmem(pmem_space, pmem_size);

    /* get RNIC */
    /* TODO: don't know how to get RNIC name from IP address directly, so this
        piece of code effectively does nothing */
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

    /* start RDMA CM */
    rdma_cm_id *raw_listen_id;
    {
        unsigned port = config.get_child("server.port").get_value<unsigned>();
        rdma_addrinfo
            hint{.ai_flags = RAI_PASSIVE, .ai_port_space = RDMA_PS_TCP},
            *info;
        if (rdma_getaddrinfo(
                addr.c_str(), std::to_string(port).c_str(),
                &hint, &info)) {
            ostringstream what;
            what << "Failed to resolve " << addr << ":" << port
                << ": " << std::strerror(errno);
            BOOST_LOG_TRIVIAL(fatal) << what.str();
            throw std::runtime_error(what.str());
        }
        defer([&] { rdma_freeaddrinfo(info); });
        ibv_qp_init_attr init_attr{
            .cap = { .max_send_wr = 16, .max_recv_wr = 16,
                        .max_send_sge = 16, .max_recv_sge = 16,
                        .max_inline_data = 512 },
            .qp_type = IBV_QPT_RC,
            .sq_sig_all = 0
        };
        if (rdma_create_ep(&raw_listen_id, info, /*qp*/NULL, &init_attr)) {
            ostringstream what;
            what << "rdma_create_ep() on " << addr << ":" << port << "failed"
                << ": " << std::strerror(errno);
            BOOST_LOG_TRIVIAL(fatal) << what.str();
            throw std::runtime_error(what.str());
        }
    }
    decltype(Server::listen_id) listen_id(raw_listen_id);

    return make_unique<Server>(
        id, config,
        std::move(managed_pmem),
        boost::asio::ip::make_address(addr), rnic_name,
        std::move(listen_id)
    );
}

Server::Server(
    unsigned _id,
    const boost::property_tree::ptree &_cfg,
    managed_pmem_t &&_pmem,
    const boost::asio::ip::address &_addr,
    const string &_rnic,
    decltype(listen_id) &&_listen_id
) : id(_id), config(_cfg),
    managed_pmem(std::move(_pmem)),
    storage(static_cast<dataslot*>(managed_pmem.buffer),
            managed_pmem.size / sizeof(dataslot)),
    addr(_addr), rnic_name(_rnic), listen_id(std::move(_listen_id)),
    ddio_guard(misc::ddio::scope_guard::from_rnic(rnic_name.c_str())),
    is_stopping(false)
{ }

Server::~Server()
{ }


/**
 * runs indefinitely unless stop() called
 *
 * 1. start listening for incoming connections
 * 2. start and block on RPC service
 * 3. try to stop when stop() is invoked
 */
void Server::run()
{
    if (rdma_listen(listen_id.get(), 0)) {
        ostringstream what;
        what << "rdma_listen(): " << std::strerror(errno);
        BOOST_LOG_TRIVIAL(error) << what.str();
        throw std::runtime_error(what.str());
    }

    while (is_stopping.load() == false) {
        // TODO:
    }
}

void Server::stop()
{
    is_stopping.store(true);
}

}   /* namespace gestalt */
