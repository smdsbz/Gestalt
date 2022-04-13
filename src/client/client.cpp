/**
 * @file client.cpp
 */

#include <fstream>
#include <cassert>
#include <algorithm>
#include <sstream>

#include <boost/property_tree/ini_parser.hpp>
#include "common/boost_log_helper.hpp"

#include "client.hpp"
#include "./ops/all.hpp"


namespace gestalt {

using namespace std;

using ReadOp = ops::Read;
using LockOp = ops::Lock; using UnlockOp = ops::Unlock;
using WriteOp = ops::WriteAPM;


Client::Client(const filesystem::path &config_path, unsigned _id) :
    id(_id),
    /* the following contexts are filled later in this constructor */
    node_mapper(), ibvctx(), session_pool()
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


Client::oloc Client::map(const okey &key, bool &need_search) const
{
    if (abnormal_placements.exist(key)) {
        [[unlikely]] need_search = false;
        return abnormal_placements.get(key);
    }
    if (normal_placements.exist(key))
        [[likely]] need_search = false;
    else {
        /* not found in both placement caches */
        need_search = true;
    }

    const auto hx = key.hash();
    const auto nodes = node_mapper.map(hx, num_replicas);

    oloc ret; ret.reserve(num_replicas);
    for (const auto &sid : nodes) {
        const auto &s = session_pool.pool.at(sid);
        const uintptr_t start_addr = s.addr + (hx % s.slots) * sizeof(dataslot);
        ret.push_back({sid, start_addr, sizeof(dataslot)});
    }

#if 0
    {
        ostringstream what;
        what << "calculated mapping for key " << key.c_str() << ", [";
        for (const auto &l : ret)
            what << "rloc(id=" << l.id << ", addr=" << l.addr << ", length=" << l.length << "), ";
        what << "]";
        BOOST_LOG_TRIVIAL(trace) << what.str();
    }
#endif

    return ret;
}

int Client::probe_and_justify_oloc(const okey &key, oloc &ls)
{
    /**
     * @note Currently we don't implement probing, as we always heat up locator
     * cache before benchmarking, and the cache is sufficiently large to hold
     * all locators for any reasonable-sized working object set.
     * It should be noted that, if run on an imbalanced deployment, lack of
     * locator justification could lead to headless overwites on secondary
     * replicas, but still, it does not affect benchmark performance, they all
     * traverse the same code path.
     */

    /* if we need to implement probing someday, use read_op, therefore we may
        save one op when probing for read on a small object */

    if (collision_set.exist(key))
        return -EDQUOT;

    normal_placements.put(key, '\0');
    return 0;
}


/* I/O interface */

int Client::raw_read(const char *key)
{
    BOOST_LOG_TRIVIAL(trace) << "Client::raw_read() object \"" << key << "\"";

    const auto prop = dynamic_cast<ReadOp*>(read_op.get());
    assert(prop);
    /* HACK: avoid further repeated construct, if not optimized */
    const okey _key(key);
    bool is_search_needed;
    auto locs = this->map(_key, is_search_needed);
    if (locs.empty()) {
        const auto what = string("cannot map key ") + key;
        [[unlikely]] throw std::runtime_error(what);
    }

    if (is_search_needed) {
        [[unlikely]] if (int r = probe_and_justify_oloc(_key, locs); r) {
            [[unlikely]] if (r == -EINVAL || r == -EDQUOT)
                return -EINVAL;
            return r;
        }
        // TODO: stuff probe read result to read_op, saving a Read op, that is if we had implemented probing
    }

    /* fetch data from remote */
    {
        const auto &loc = locs[0];
        const auto &mr = session_pool.pool.at(loc.id);
        if (int r = (*prop)(mr.conn.get(), loc.addr, loc.length, mr.rkey)(); r)
            [[unlikely]] return r;
    }

    /* validate data on your own */
    read_op->buf.pos = 0;

    return 0;
}

int Client::get(const char *key)
{
    if (int r = raw_read(key); r)
        [[unlikely]] return r;
    int v = read_op->buf.validity(key);
    if (v == 0)
        [[likely]] return 0;

    if (v == -EINVAL) {
        erase_oloc_cache(key);
        return -EINVAL;
    }

    /* buffer cannot hold all data */
    if (v == -EOVERFLOW)
        return -EOVERFLOW;

    /* multi-slot object, this should be fixed at probing phase, unless it is
        changed after probing before fetching */
    if (v == -EREMOTE) {
        [[unlikely]] erase_oloc_cache(key);
        return -EREMOTE;
    }

    return v;
}

int Client::put(void)
{
    BOOST_LOG_TRIVIAL(trace) << "Client::put() object "
        << write_op->buf.arr[0].key().c_str() << " of size "
        << write_op->buf.size() << "B (" << write_op->buf.slots() << " slots)";

    const auto plop = dynamic_cast<LockOp*>(lock_op.get());
    const auto pulop = dynamic_cast<UnlockOp*>(unlock_op.get());
    const auto pwop = dynamic_cast<WriteOp*>(write_op.get());
    assert(plop && pulop && pwop);

    /**
     * @note currently large value support not implemented
     */
    if (pwop->buf.slots() > 1)
        [[unlikely]] throw std::runtime_error("large object not supported yet");

    const auto &_key = pwop->buf.data()[0].key();
    bool is_search_needed;
    auto locs = this->map(_key, is_search_needed);

    /* justify replica location */

    /**
     * If test during placement justification, which is essentially lock dry-run,
     * failed, could be
     *  1. an invalid slot
     *      a) cluster shifted
     *      b) data elsewhere in linear search range
     *      c) ok to alloc for object creation (if not b)
     *  2. key (fingerprint) does not match
     *      a) cluster shifted
     *      b) data elsewhere in linear search range
     *      c) collision on create, bucket near full
     *  3. slot number mismatch
     *      a) data slots layout changed, i.e. resized
     *  4. (valid and key match, but) object locked
     *      a) write locked, try again later
     *
     * Cluster shift is prevented at bucket level, that is, once RDMA connections
     * are initialized, we make sure the memory pool layout of a bucket remains
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

    if (is_search_needed) {
        [[unlikely]] if (int r = probe_and_justify_oloc(_key, locs); r) {
            [[unlikely]] if (r == -EINVAL) {
                [[likely]] /* should insert, do nothing */;
            }
            else {
                if (r == -EDQUOT) {
                    [[likely]] return -EDQUOT;
                }
                return r;
            }
        }
    }

    /* initialize replica vector */

    vector<WriteOp::target_t> repvec;
    for (const auto &r : locs) {
        const auto &m = session_pool.pool.at(r.id);
        repvec.push_back({m.conn.get(), r.addr, m.rkey});
    }
    const auto &prim_rep = repvec.at(0);

    /* lock (primary) */
    do {
        if (int r = (*plop)(prim_rep.id, prim_rep.addr, _key, prim_rep.rkey)(); r) {
            [[unlikely]] if (r == -EINVAL)
                [[likely]] break;
            if (r == -EBADF) {
                [[likely]] collision_set.put(_key, '\0');
                erase_oloc_cache(_key);
                return -EDQUOT;
            }
            return r;
        }
    } while (0);

    /* write replicas
        HACK: we ignore lock status on replicas, write whatever comes handy as
        long as it is consistent */
    /* NOTE: unlock step skipped for non-replicated buckets */
    {
        if (int r = (*pwop)(repvec, repvec.size() != 1)(); r)
            [[unlikely]] return r;
    }

    /* unlock (primary) */
    do {
        if (repvec.size() == 1)
            break;

        if (int r = (*pulop)(prim_rep.id, prim_rep.addr, _key, prim_rep.rkey)(); r)
            [[unlikely]] return r;
    } while (0);

    return 0;
}

}   /* namespace gestalt */
