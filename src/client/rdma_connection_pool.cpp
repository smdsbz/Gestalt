/**
 * @file rdma_connection_pool.cpp
 *
 * Manages RDMA connections to any server node in cluster, proxies I/O operations
 * to the correct node.
 */

#include <filesystem>
#include <string>
#include <sstream>
#include <arpa/inet.h>

#include "common/boost_log_helper.hpp"

#include <grpcpp/channel.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/client_context.h>
#include "Session.pb.h"
#include "Session.grpc.pb.h"

#include "internal/rdma_connection_pool.hpp"
#include "client.hpp"
#include "internal/data_mapper.hpp"
#include "common/defer.hpp"


namespace gestalt {

using namespace std;


RDMAConnectionPool::RDMAConnectionPool(Client *_c) : client(_c)
{
    using ServerStatus = DataMapper::server_node::Status;
    const auto srv_rpc_port =
        client->config.get_child("server.rpc_port").get_value<unsigned>();
    const auto srv_rdma_port =
        client->config.get_child("server.rdma_port").get_value<unsigned>();

    for (const auto &[server_id, s] : client->node_mapper.server_map) {
        BOOST_LOG_TRIVIAL(trace) << "try connecting server "
            << server_id << " @ " << s.addr
            << " (port rpc " << srv_rpc_port << " rdma " << srv_rdma_port << ")";
        if (s.status != ServerStatus::up) {
            BOOST_LOG_TRIVIAL(warning) << __func__ << "(): "
                << "stumbled on an inactive server in cluster map, ignoring";
            continue;
        }

        /* 1. initiate call */
        auto chan = grpc::CreateChannel(
            s.addr + ":" + std::to_string(srv_rpc_port),
            grpc::InsecureChannelCredentials());
        auto stub = gestalt::rpc::Session::NewStub(chan);
        grpc::ClientContext ctx;
        gestalt::rpc::ClientProp in;
        in.set_id(client->id);
        auto reader = stub->Connect(&ctx, in);

        /* 2. connect */
        rdma_cm_id *raw_conn;
        {
            rdma_addrinfo *addrinfo;
            rdma_addrinfo addr_hint{
                .ai_port_space = RDMA_PS_TCP
            };
            if (rdma_getaddrinfo(
                    s.addr.c_str(), std::to_string(srv_rdma_port).c_str(),
                    &addr_hint, &addrinfo))
                boost_log_errno_throw(rdma_getaddrinfo);
            defer([&] { rdma_freeaddrinfo(addrinfo); });
            ibv_qp_init_attr init_attr{
                .cap = { .max_send_wr = 16, .max_recv_wr = 16,
                            .max_send_sge = 16, .max_recv_sge = 16,
                            .max_inline_data = 512 },
                .qp_type = IBV_QPT_RC,
                .sq_sig_all = 0
            };
            if (rdma_create_ep(&raw_conn, addrinfo, client->ibvpd.get(), &init_attr))
                boost_log_errno_throw(rdma_create_ep);
            if (rdma_connect(raw_conn, NULL)) {
                BOOST_LOG_TRIVIAL(warning) << "Cannot connect to server "
                    << server_id << " @ " << s.addr << ", marking it out";
                rdma_destroy_ep(raw_conn);
                client->node_mapper.mark_out(server_id);
                continue;
            }
            BOOST_LOG_TRIVIAL(trace) << "RDMA connected to "
                << inet_ntoa(raw_conn->route.addr.dst_sin.sin_addr)
                << ":" << raw_conn->route.addr.dst_sin.sin_port
                << ", local port "
                << inet_ntoa(raw_conn->route.addr.src_sin.sin_addr)
                << ":" << raw_conn->route.addr.src_sin.sin_port;
        }
        decltype(memory_region::conn) conn(raw_conn);

        /* 3. get MR from server */
        gestalt::rpc::MemoryRegion raw_mr;
        if (!reader->Read(&raw_mr)) {
            const char *what = "failed reading memory region from server";
            BOOST_LOG_TRIVIAL(fatal) << what;
            throw std::runtime_error(what);
        }
        memory_region mr(
            raw_mr.addr(), raw_mr.length(), raw_mr.rkey(),
            std::move(conn));

        /* 4. finish */
        if (auto r = reader->Finish(); !r.ok()) {
            auto what = string("closing RPC Connect(): ") + r.error_message();
            BOOST_LOG_TRIVIAL(fatal) << what;
            throw std::runtime_error(what);
        }

        /* 5. add connection property to runtime */
        pool.insert({server_id, std::move(mr)});
        BOOST_LOG_TRIVIAL(trace) << "inserted server " << server_id
            << " to connection pool";
    }
}


RDMAConnectionPool::~RDMAConnectionPool() noexcept(false)
{
    const auto srv_rpc_port =
        client->config.get_child("server.rpc_port").get_value<unsigned>();
    grpc::CompletionQueue grpccq;

    /* disconnect one-by-one */
    for (auto mrit = pool.begin(); mrit != pool.end(); mrit = pool.begin()) {
        const auto &[server_id, mr] = *mrit;
        const auto &conn = mr.conn;

        BOOST_LOG_TRIVIAL(trace) << "RDMA disconnecting from "
            << inet_ntoa(conn->route.addr.dst_sin.sin_addr)
            << ":" << conn->route.addr.dst_sin.sin_port;

        auto chan = grpc::CreateChannel(
            client->node_mapper.server_map.at(server_id).addr
                + ":" + std::to_string(srv_rpc_port),
            grpc::InsecureChannelCredentials());
        auto stub = gestalt::rpc::Session::NewStub(chan);
        grpc::ClientContext ctx;
        gestalt::rpc::ClientProp in;
        in.set_id(client->id);
        google::protobuf::Empty out;
        grpc::Status r;
        stub->AsyncDisconnect(&ctx, in, &grpccq)
            ->Finish(&out, &r, reinterpret_cast<void*>(server_id));

        /* rdma_disconnect() at client side, invoked automatically with dtor */
        pool.erase(mrit);
        /* NOTE: sometimes it takes a while ... */

        void *tag;
        bool ok = false;
        if (grpccq.Next(&tag, &ok); !ok) {
            auto what = string("RPC Disconnect(): ") + r.error_message();
            BOOST_LOG_TRIVIAL(fatal) << what;
            throw std::runtime_error(what);
        }
    }
}

}   /* namespace gestalt */
