/**
 * @file server.cpp
 *
 * Implementation of the server instance.
 */

#include <sstream>
#include <fstream>
#include <thread>
#include <chrono>
using namespace std::chrono_literals;

#include "common/boost_log_helper.hpp"
#include <boost/property_tree/ini_parser.hpp>

#include <libpmem.h>
#include <rdma/rdma_cma.h>
#include <grpcpp/channel.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/client_context.h>
#include "ClusterMap.pb.h"
#include "ClusterMap.grpc.pb.h"
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/security/server_credentials.h>

#include "./server.hpp"
#include "misc/numa.hpp"
#include "misc/ddio.hpp"
#include "common/defer.hpp"
#include "./session_servicer.hpp"


namespace gestalt {
using namespace std;

namespace {
struct __IbvPdDeleter {
    inline void operator()(ibv_pd *pd)
    {
        if (errno = ibv_dealloc_pd(pd); errno)
            boost_log_errno_throw(ibv_dealloc_pd);
    }
};
}

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
            what << "Failed to map DEVDAX at " << dax_path << ": " << std::strerror(errno);
            BOOST_LOG_TRIVIAL(fatal) << what.str();
            throw std::runtime_error(what.str());
        }
    }
    managed_pmem_t managed_pmem(pmem_space, pmem_size);

    /* get RNIC */
    /* TODO: don't know how to get RNIC name from IP address directly, for now
        we just use whatever we got */
    ibv_context **raw_devices, *rnic_chosen;
    {
        raw_devices = rdma_get_devices(NULL);
        if (!raw_devices) {
            BOOST_LOG_TRIVIAL(fatal) << "No RNIC found!";
            throw std::runtime_error("no RNIC");
        }
        rnic_chosen = gestalt::misc::numa::choose_rnic_on_same_numa(
            dax_path.filename().c_str(), raw_devices);
        if (!rnic_chosen) {
            BOOST_LOG_TRIVIAL(warning) << "Cannot find a matching RNIC on the "
                << "same NUMA as the DEVDAX, using the first RNIC listed "
                << "instead!";
            rnic_chosen = raw_devices[0];
        }
    }
    managed_ibvctx_t ibvctx(raw_devices);
    ibvctx.chosen = rnic_chosen;

    /* register memory region */
    BOOST_LOG_TRIVIAL(info) << "Registering PMem " << dax_path
        << " to RNIC " << ibvctx.chosen->device->name
        << ", this may take a while ...";
    unique_ptr<ibv_pd, __IbvPdDeleter> pd(ibv_alloc_pd(ibvctx.chosen));
    if (!pd)
        boost_log_errno_throw(ibv_alloc_pd);
    decltype(Server::ibvmr) ibvmr(ibv_reg_mr(
        pd.get(),
        managed_pmem.buffer, managed_pmem.size,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC
    ));
    if (!ibvmr)
        boost_log_errno_throw(ibv_reg_mr);
    BOOST_LOG_TRIVIAL(info) << "Successfully registered memory region!";

    /* start RDMA CM */
    rdma_cm_id *raw_listen_id;
    {
        unsigned port = config.get_child("server.rdma_port").get_value<unsigned>();
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
            .cap = { .max_send_wr = 1024, .max_recv_wr = 1024,
                        .max_send_sge = 16, .max_recv_sge = 16,
                        .max_inline_data = 512 },
            .qp_type = IBV_QPT_RC,
            .sq_sig_all = 0
        };
        if (rdma_create_ep(&raw_listen_id, info, pd.get(), &init_attr)) {
            ostringstream what;
            what << "rdma_create_ep() on " << addr << ":" << port << " failed: "
                << std::strerror(errno);
            BOOST_LOG_TRIVIAL(fatal) << what.str();
            throw std::runtime_error(what.str());
        }
    }
    decltype(Server::listen_id) listen_id(raw_listen_id);
    pd.release();

    return make_unique<Server>(
        id, config,
        std::move(managed_pmem),
        boost::asio::ip::make_address(addr),
        std::move(ibvctx), std::move(ibvmr),
        std::move(listen_id)
    );
}

Server::Server(
    unsigned _id,
    const boost::property_tree::ptree &_cfg,
    managed_pmem_t &&_pmem,
    const boost::asio::ip::address &_addr,
    decltype(ibvctx) &&_ibvctx, decltype(ibvmr) &&_ibvmr,
    decltype(listen_id) &&_listen_id
) : id(_id), config(_cfg),
    managed_pmem(std::move(_pmem)),
    storage(static_cast<dataslot*>(managed_pmem.buffer),
            managed_pmem.size / sizeof(dataslot)),
    addr(_addr), ibvctx(std::move(_ibvctx)), ibvmr(std::move(_ibvmr)),
    listen_id(std::move(_listen_id)),
    ddio_guard(misc::ddio::scope_guard::from_rnic(ibvctx.chosen->device->name)),
    is_stopping(false)
{
    BOOST_LOG_TRIVIAL(info) << "cleaning storage, this may take a while ...";
    BOOST_LOG_TRIVIAL(debug) << "storage.capacity() = " << storage.capacity();
    storage.clear();
    pmem_msync(managed_pmem.buffer, managed_pmem.size);
    BOOST_LOG_TRIVIAL(info) << "Server successfully initialized!";
}

Server::~Server()
{
    stop();
    /* CMBK: unregister from monitor, for now we don't bother  */
}


/**
 * runs indefinitely unless stop() called
 *
 * 1. start listening for incoming connections
 * 2. start and block on RPC service
 * 3. try to stop when stop() is invoked
 */
void Server::run()
{
    /* start listening */
    if (rdma_listen(listen_id.get(), 0))
        boost_log_errno_throw(rdma_listen);

    /* start RPC service */
    gestalt::rpc::SessionServicer session_svc(this);
    grpc::ServerBuilder grpc_builder;
    {
        ostringstream port;
        port << this->addr << ":"
            << config.get_child("server.rpc_port").get_value<unsigned>();
        grpc_builder.AddListeningPort(port.str(), grpc::InsecureServerCredentials());
        BOOST_LOG_TRIVIAL(info) << "starting RPC server on " << port.str();
    }
    grpc_builder.RegisterService(&session_svc);
    auto session_grpc_server = grpc_builder.BuildAndStart();
    if (!session_grpc_server) {
        BOOST_LOG_TRIVIAL(fatal) << "RPC server failed to start";
        throw std::runtime_error("RPC server failed to start");
    }

    BOOST_LOG_TRIVIAL(info) << "Server up and running!";

    while (is_stopping.load() == false) {
        std::this_thread::sleep_for(1s);
    }
    session_grpc_server->Wait();

    BOOST_LOG_TRIVIAL(info) << "Server stopped!";
}

void Server::stop()
{
    is_stopping.store(true);
}

}   /* namespace gestalt */
