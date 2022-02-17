/**
 * @file client.cpp
 */

#include <fstream>
#include <cassert>
#include <algorithm>

#include <boost/property_tree/ini_parser.hpp>
#include "common/boost_log_helper.hpp"

#include "client.hpp"
#include "./ops/all.hpp"


namespace gestalt {

using namespace std;

using ReadOp = ops::Read;
using LockOp = ops::Lock; using UnlockOp = ops::Unlock;
using WriteOp = ops::WriteAPM;


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
    unlock_op.reset(new UnlockOp(ibvpd.get()));
    write_op.reset(new WriteOp(ibvpd.get()));
}


Client::oloc Client::map(const okey &key, bool &need_search)
{
    if (abnormal_placements.exist(key)) {
        [[unlikely]] need_search = false;
        return abnormal_placements.get(key);
    }

    need_search = true;
    const auto hx = key.hash();
    const auto nodes = node_mapper.map(hx, num_replicas);

    oloc ret; ret.reserve(num_replicas);
    for (const auto &sid : nodes) {
        const auto &s = session_pool.pool.at(sid);
        const uintptr_t start_addr = s.addr + (hx % s.slots) * params::data_seg_length;
        ret.push_back({sid, start_addr, params::data_seg_length});
    }

    return ret;
}


/* I/O interface */

void Client::raw_read(const char *key)
{
    auto prop = dynamic_cast<ReadOp*>(read_op.get());
    assert(prop);
    /* HACK: avoid further repeated construct, if not optimized */
    const okey _key(key);
    bool is_search_needed;
    const auto locs = this->map(_key, is_search_needed);
    if (locs.empty()) {
        const auto what = string("cannot map key ") + key;
        [[unlikely]] throw std::runtime_error(what);
    }

    /* fetch data from remote */
    {
        const auto &loc = locs[0];
        const auto &mr = session_pool.pool.at(loc.id);
        const uint32_t search_span =
            is_search_needed ?
            std::min(
                loc.length + (params::hht_search_length - 1) * sizeof(dataslot),
                mr.length - loc.addr
            ) :
            loc.length;
        int r = (*prop) (mr.conn.get(), loc.addr, search_span, mr.rkey) ();
        if (r) {
            const auto what = string("RDMA op threw: ") + std::strerror(-r);
            [[unlikely]] throw std::runtime_error(what);
        }
    }

    /* validate data on your own */
    read_op->buf.pos = 0;
}

int Client::get(const char *key)
{
    raw_read(key);
    int v = read_op->buf.validity(key);
    if (v == 0)
        [[likely]] return 0;

    if (v == -EINVAL)
        return -EINVAL;

    if (v == -EREMOTE) {
        // TODO: multi-slot object
        throw std::runtime_error("long value support not implemented yet");
    }
    if (v == -EOVERFLOW) {
        // TODO: segmented read for really large data
        throw std::runtime_error("long value support not implemented yet");
    }

    throw std::runtime_error("unreachable");
}

int Client::put(const char *k, void *d, size_t dl)
{
    auto pwop = dynamic_cast<WriteOp*>(write_op.get());
    assert(pwop);

    pwop->buf.set(k, d, dl);

    return put();
}

int Client::put(void)
{
    auto plop = dynamic_cast<LockOp*>(lock_op.get());
    auto pulop = dynamic_cast<UnlockOp*>(unlock_op.get());
    auto pwop = dynamic_cast<WriteOp*>(write_op.get());
    assert(plop && pulop && pwop);

    /**
     * @note currently large value support not implemented
     */
    if (pwop->buf.slots() > 1)
        [[unlikely]] throw std::runtime_error("large object not supported yet");

    const auto &_key = pwop->buf.data()[0].key();
    bool is_search_needed;
    const auto locs = this->map(_key, is_search_needed);

    /* justify replica location */

    vector<WriteOp::target_t> vec;  // replica channel vector
    for (const auto &r : locs) {
        const auto &m = session_pool.pool.at(r.id);
        vec.push_back({m.conn.get(), r.addr, m.rkey});
    }

    /**
     * @note
     * If test during placement justification, which is essentially lock dry-run,
     * failed, could be
     *  1. an invalid slot
     *      a) cluster shifted
     *      b) data elsewhere in linear search range
     *      c) ok to alloc for object creation (if not b)
     *  2. key (fingerprint) does not match
     *      a) cluster shifted
     *      b) data elsewhere in linear search range
     *      b) collision on create, bucket near full
     *  3. (valid and key match, but) object locked
     *      a) write locked, try again later
     *
     * Cluster shift is prevented at bucket level, that is, once RDMA connections
     * are initialized, we make sure the data placement within a bucket remains
     * static.
     *
     * Moreover, in the current implementation, linear search is not implemented,
     * therefore a lock fail due to invalid always means object not exist, and
     * key mismatch always means collision.
     * @sa Client::abnormal_placements
     *
     * Therefore, linear search on write does not need to be implemented, at
     * least for now. What comes out of Client::map(const okey&, bool&) is where
     * data goes to. Collision on primary means failure, and collision on replicas
     * is ignored (as this implementation is only intended for performance
     * benchmarking) !
     */
    is_search_needed = false;
    if (is_search_needed)
        [[unlikely]] throw std::runtime_error("not supported yet");

    /* lock (primary) */
    {
        const auto &prim = vec.at(0);
        if (int r = (*plop)(prim.id, prim.addr, _key, prim.rkey)(); r) {
            [[unlikely]] if (r == -ECANCELED)
                [[likely]] return -EDQUOT;
            return r;
        }
    }

    /* write primary (write with lock preserved on the 1st order, cleared on rest) */
    {
        if (int r = (*pwop)(vec, /*primary?*/true)(); r)
            [[unlikely]] return r;
    }

    /* write secondary (write with lock cleared) */
    {
        if (int r = (*pwop)(vec, /*primary?*/false)(); r)
            [[unlikely]] return r;
    }

    /* unlock (primary) */
    {
        const auto &prim = vec.at(0);
        if (int r = (*pulop)(prim.id, prim.addr, _key, prim.rkey)(); r)
            [[unlikely]] return r;
    }

    return 0;
}

}   /* namespace gestalt */
