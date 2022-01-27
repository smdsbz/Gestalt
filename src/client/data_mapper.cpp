/**
 * @file data_mapper.cpp
 */

#include "common/boost_log_helper.hpp"

#include <grpcpp/channel.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/client_context.h>
#include "ClusterMap.pb.h"
#include "ClusterMap.grpc.pb.h"

#include "client.hpp"
#include "./data_mapper.hpp"


namespace gestalt {

using namespace std;


DataMapper::DataMapper(Client *_c) : client(_c)
{
    gestalt::rpc::ServerList out;
    {
        auto chan = grpc::CreateChannel(
            client->config.get_child("global.monitor_address").get_value<string>(),
            grpc::InsecureChannelCredentials());
        auto stub = gestalt::rpc::ClusterMap::NewStub(chan);
        grpc::ClientContext ctx;
        if (auto r = stub->GetServers(&ctx, {}, &out); !r.ok())
            boost_log_errno_throw(GetServers);
    }
    BOOST_LOG_TRIVIAL(info) << "fetched server list from monitor";

    const auto &servers = out.servers();
    server_list.resize(servers.size());
    for (const auto &s : servers) {
        if (s.id() >= server_list.size())
            server_list.resize(s.id() + 1);
        server_list[s.id()] = server_node(s.addr());
    }
}


DataMapper::acting_set DataMapper::map(const string &k, unsigned r)
{
    unsigned base = okey::hash(k) % server_list.size();
    acting_set out;
    for (unsigned off = 0; off < server_list.size() && out.size() < r; ++off) {
        unsigned i;
        [[likely]] i = (off + base) % server_list.size();
        if (server_list[i].status != server_node::Status::up)
            [[unlikely]] continue;
        out.push_back(i);
    }
    return out;
}

}   /* namespace gestalt */
