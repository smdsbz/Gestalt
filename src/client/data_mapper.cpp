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
#include "internal/data_mapper.hpp"


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
        /**
         * TODO: should be getting only servers responsible for bucket opened by
         * this client, however in the benchmark setup we only have one big
         * bucket that takes all PMem space, so for now we get all servers in
         * the cluster.
         */
        if (auto r = stub->GetServers(&ctx, {}, &out); !r.ok()) {
            string what = string("RPC GetServers(): ") + r.error_message();
            BOOST_LOG_TRIVIAL(fatal) << what;
            throw std::runtime_error(what);
        }
    }
    BOOST_LOG_TRIVIAL(debug) << "fetched server list from monitor";

    const auto &servers = out.servers();
    server_rank.reserve(servers.size());
    for (const auto &s : servers) {
        server_rank.push_back(s.id());
        server_map.insert({s.id(), {s.addr()}});
    }
}

DataMapper::acting_set DataMapper::map(uint32_t base, unsigned r) const
{
    /**
     * @note currently implemented as round-robin
     */
    acting_set out;
    for (unsigned off = 0; off < server_rank.size() && out.size() < r; ++off) {
        unsigned rank;
        [[likely]] rank = (off + base) % server_rank.size();
        unsigned id = server_rank[rank];
        if (server_map.at(id).status != server_node::Status::up)
            [[unlikely]] continue;
        out.push_back(id);
    }
    return out;
}

void DataMapper::mark_out(unsigned id)
{
    server_map.at(id).status = server_node::Status::out;
}

}   /* namespace gestalt */
