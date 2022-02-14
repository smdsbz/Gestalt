/**
 * @file client.cpp
 */

#include <fstream>

#include <boost/property_tree/ini_parser.hpp>
#include "common/boost_log_helper.hpp"

#include "client.hpp"
#include "./ops/all.hpp"


namespace gestalt {

using namespace std;

using ReadOp = ops::Read;
using LockOp = ops::Lock;


Client::Client(const filesystem::path &config_path) :
    id(/*TODO: random generate, or allocated by monitor*/114514),
    node_mapper(), ibvctx(), session_pool(),
    abnormal_placements(gestalt::defaults::client_redirection_cache_size)
{
    {
        ifstream f(config_path);
        boost::property_tree::read_ini(f, config);
    }
    num_replicas = config.get_child("global.num_replicas").get_value<unsigned>();
    if (!num_replicas)
        throw std::invalid_argument("num_replicas");

    node_mapper = DataMapper(this);
    BOOST_LOG_TRIVIAL(debug) << "DataMapper initialized: "
        << node_mapper.dump_clustermap();

    /* get global RDMA PD */
    {
        ibvctx.devices = rdma_get_devices(NULL);
        if (!ibvctx.devices) {
            BOOST_LOG_TRIVIAL(fatal) << "No RNIC found!";
            throw std::runtime_error("no RNIC");
        }
        ibvctx.chosen = ibvctx.devices[0];

        ibvpd.reset(ibv_alloc_pd(ibvctx.chosen));
        if (!ibvpd)
            boost_log_errno_throw(ibv_alloc_pd);
    }

    session_pool = RDMAConnectionPool(this);
    BOOST_LOG_TRIVIAL(debug) << "RDMAConnectionPool initialized";

    /* initialize structured RDMA ops */
    read_op.reset(new ReadOp(ibvpd.get()));
    lock_op.reset(new LockOp(ibvpd.get()));
}


Client::oloc Client::map(const okey &key)
{
    if (abnormal_placements.exist(key))
        [[unlikely]] return abnormal_placements.get(key);

    const auto hx = key.hash();
    const auto nodes = node_mapper.map(hx, num_replicas);

    oloc ret;
    for (const auto &sid : nodes) {
        const auto &s = session_pool.pool.at(sid);
        const uintptr_t start_addr = s.addr + (hx % s.slots) * params::data_seg_length;
        ret.push_back({sid, start_addr, params::data_seg_length});
    }

    return ret;
}


/* I/O interface */

void Client::get(const char *key)
{
    auto pop = dynamic_cast<ReadOp*>(read_op.get());
    if (!pop)
        [[unlikely]] throw std::runtime_error("bad cast to ops::Read");
    /* HACK: avoid further repeated construct, if not optimized */
    const okey _key(key);
    const auto locs = this->map(_key);
    if (locs.empty()) {
        const auto what = string("cannot map key ") + key;
        [[unlikely]] throw std::runtime_error(what);
    }

    /* fetch data from remote */
    {
        auto &op = *pop;
        const auto &loc = locs[0];
        const auto &mr = session_pool.pool.at(loc.id);
        int r = op (mr.conn.get(), loc.addr, loc.length, mr.rkey) ();
        if (r) {
            const auto what = string("RDMA op threw: ") + std::strerror(-r);
            [[unlikely]] throw std::runtime_error(what);
        }
    }

    /* validate data on your own */
}

}   /* namespace gestalt */
