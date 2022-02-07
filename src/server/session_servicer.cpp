/**
 * @file session_servicer.cpp
 */

#include <sstream>
#include <type_traits>
#include <arpa/inet.h>
#include <iomanip>

#include "common/boost_log_helper.hpp"

#include "./session_servicer.hpp"
#include "./server.hpp"


namespace gestalt {
namespace rpc {

using namespace std;


Status SessionServicer::Connect(ServerContext *ctx,
    const ClientProp *in, ServerWriter<MemoryRegion> *out)
{
    BOOST_LOG_TRIVIAL(trace) << "RPC Session::Connect() invoked";
    std::scoped_lock l(server->_mutex);

    auto &connected_clients = server->connected_client_id;

    /* reject re-connect */
    if (connected_clients.contains(in->id())) {
        BOOST_LOG_TRIVIAL(warning) << "client " << in->id() << " already exists, ignoring";
        return Status(StatusCode::ALREADY_EXISTS, "client already connected");
    }

    /* 2. accept connection */
    rdma_cm_id *raw_connected_id;
    if (rdma_get_request(server->listen_id.get(), &raw_connected_id))
        boost_log_errno_grpc_return(rdma_get_request);
    unique_ptr<rdma_cm_id, gestalt::Server::__RdmaConnDeleter> connected_id(raw_connected_id);
    if (connected_id->pd != server->listen_id->pd) {
        BOOST_LOG_TRIVIAL(fatal) << "connection not using pre-allocated PD!";
        return Status(StatusCode::INTERNAL, "");
    }
    /* NOTE: client should be issuing rdma_connect(). For a proof-of-concept
        implementation, we wait indefinitely */
    if (rdma_accept(connected_id.get(), NULL))
        boost_log_errno_grpc_return(rdma_accept);
    BOOST_LOG_TRIVIAL(trace) << "accepted RDMA connection from "
        << inet_ntoa(connected_id->route.addr.dst_sin.sin_addr)
        << ":" << connected_id->route.addr.dst_sin.sin_port
        << ", with local port "
        << inet_ntoa(connected_id->route.addr.src_sin.sin_addr)
        << ":" << connected_id->route.addr.src_sin.sin_port;

    // /* 3. register MR */
    // BOOST_LOG_TRIVIAL(trace) << "ibv_reg_mr(): "
    //     << std::hex << server->managed_pmem.buffer
    //     << std::dec << " of " << server->managed_pmem.size << " bytes";
    // ibv_mr *raw_mr = ibv_reg_mr(connected_id->pd,
    //     server->managed_pmem.buffer, server->managed_pmem.size,
    //     IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
    //     IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
    // if (!raw_mr)
    //     boost_log_errno_grpc_return(ibv_reg_mr);
    // unique_ptr<ibv_mr, gestalt::Server::__IbvMrDeleter> mr(raw_mr);

    /* 3. write MR fields to stream */
    {
        MemoryRegion o;
        const auto &mr = server->ibvmr;
        o.set_addr(reinterpret_cast<uintptr_t>(mr->addr));
        o.set_length(mr->length);
        o.set_rkey(mr->rkey);
        out->Write(o);
    }

    /* 4. client connection now initialized, add it to server runtime registry */
    connected_clients.insert({
        in->id(),
        gestalt::Server::client_prop_t(std::move(connected_id))
    });

    BOOST_LOG_TRIVIAL(info) << "client " << in->id() << " connected";
    return Status::OK;
}

Status SessionServicer::Disconnect(ServerContext *ctx,
    const ClientProp *in, Empty *out)
{
    std::scoped_lock l(server->_mutex);

    auto &connected_clients = server->connected_client_id;

    auto client_it = server->connected_client_id.find(in->id());

    /* ignore if not seen */
    if (client_it == connected_clients.end()) {
        BOOST_LOG_TRIVIAL(warning) << "cannot disconnect client " << in->id()
            << " for it's not connected yet, ignoring";
        return Status::OK;
    }

    connected_clients.erase(client_it);

    BOOST_LOG_TRIVIAL(info) << "client " << in->id() << " disconnected";
    return Status::OK;
}

}   /* namespace rpc */
}   /* namespace gestalt */
