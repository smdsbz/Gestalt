/**
 * @file session_servicer.cpp
 */

#include <sstream>
#include <type_traits>

#include "common/boost_log_helper.hpp"

#include "./session_servicer.hpp"
#include "./server.hpp"


namespace gestalt {
namespace rpc {

using namespace std;


Status SessionServicer::Connect(ServerContext *ctx,
    const ClientProp *in, ServerWriter<MemoryRegion> *out)
{
    std::scoped_lock l(server->_mutex);

    auto &connected_clients = server->connected_client_id;

    /* reject re-connect */
    if (in->id() && connected_clients.contains(in->id())) {
        BOOST_LOG_TRIVIAL(warning) << "client " << in->id() << " already exists, ignoring";
        return Status(StatusCode::ALREADY_EXISTS, "client already connected");
    }

    /* accept connection */
    rdma_cm_id *raw_connected_id;
    if (rdma_get_request(server->listen_id.get(), &raw_connected_id))
        boost_log_errno_throw(rdma_get_request);
    unique_ptr<rdma_cm_id, gestalt::Server::__RdmaConnDeleter> connected_id(raw_connected_id);
    /* CMBK: client should be issuing rdma_connect(). For a proof-of-concept
        implementation, we wait indefinitely */
    if (rdma_accept(connected_id.get(), NULL))
        boost_log_errno_throw(rdma_accept);

    /* register MR */
    ibv_mr *raw_mr = ibv_reg_mr(connected_id->pd,
        server->managed_pmem.buffer, server->managed_pmem.size,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
    if (!raw_mr)
        boost_log_errno_throw(ibv_reg_mr);
    unique_ptr<ibv_mr, gestalt::Server::__IbvMrDeleter> mr(raw_mr);

    /* write MR fields to stream */
    {
        MemoryRegion o;
        o.set_addr(reinterpret_cast<uintptr_t>(mr->addr));
        o.set_length(mr->length);
        o.set_rkey(mr->rkey);
        out->Write(o);
    }

    /* client connection now initialized, add it to server runtime registry */
    {
        gestalt::Server::client_prop_t cp(std::move(connected_id), std::move(mr));
        std::remove_reference_t<decltype(connected_clients)>::value_type v(in->id(), std::move(cp));
        connected_clients.insert(std::move(v));
    }

    BOOST_LOG_TRIVIAL(info) << "client @ " << in->id() << " connected";
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

    BOOST_LOG_TRIVIAL(info) << "client @ " << in->id() << " disconnected";
    return Status::OK;
}

}   /* namespace rpc */
}   /* namespace gestalt */
