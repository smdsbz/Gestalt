/**
 * @file client.hpp
 *
 * @note All HA features not implemented.
 * @see src/server/server.hpp
 */

#pragma once

#include <filesystem>
#include <unordered_map>
#include <vector>

#include <rdma/rdma_cma.h>
#include "common/boost_log_helper.hpp"
#include <boost/property_tree/ini_parser.hpp>
#include <boost/noncopyable.hpp>

#include "./spec/dataslot.hpp"
#include "./internal/ops_base.hpp"
#include "./internal/data_mapper.hpp"
#include "./internal/rdma_connection_pool.hpp"
#include "./common/lru_cache.hpp"
#include "./defaults.hpp"


namespace gestalt {

using namespace std;

using okey = dataslot::key_type;
class DataMapper;
class RDMAConnectionPool;


/**
 * Client - the Gestalt storage cluster operator
 *
 * @note Not thread-safe, for shared access is controlled accross client by
 * design, and tackling with inter-thread synchronization would hurt performance
 * in the common case.
 */
class Client final : private boost::noncopyable {

    /* instance runtime */

    unsigned id;
    boost::property_tree::ptree config;
    /**
     * number of replicas of the bucket, read-only
     */
    unsigned num_replicas;

    /* cluster */

    /** maps from okey to server nodes in cluster */
    DataMapper node_mapper;
    friend class DataMapper;

    /* RDMA sessions */

    /** ibv_context for #ibvpd */
    struct managed_ibvctx_t : private boost::noncopyable {
        /** null-terminated array */
        ibv_context **devices = NULL;
        ibv_context *chosen = NULL;
    public:
        ~managed_ibvctx_t()
        {
            if (!devices)
                return;
            rdma_free_devices(devices);
        }
    } ibvctx;
    struct __IbvPdDeleter {
        inline void operator()(ibv_pd *pd)
        {
            if (ibv_dealloc_pd(pd))
                boost_log_errno_throw(ibv_dealloc_pd);
        }
    };
    unique_ptr<ibv_pd, __IbvPdDeleter> ibvpd;
    struct __IbvCqDeleter {
        inline void operator()(ibv_cq *cq)
        {
            if (ibv_destroy_cq(cq))
                boost_log_errno_throw(ibv_destroy_cq);
        }
    };
    /**
     * shared RDMA Completion Queue
     * @sa gestalt::optimization::batched_poll
     */
    unique_ptr<ibv_cq, __IbvCqDeleter> ibvscq;
    /** pooled RDMA connection */
    RDMAConnectionPool session_pool;
    friend class RDMAConnectionPool;

    /* misc */

    struct cluster_physical_addr {
        /** server ID */
        unsigned id;
        /** starting VA of requested value on that server */
        uintptr_t addr;
        /** length (in bytes) of the object, multiples of sizeof(dataslot) */
        uint32_t length;
    public:
        cluster_physical_addr(unsigned _id, uintptr_t _addr, uint32_t _length) noexcept :
            id(_id), addr(_addr), length(_length)
        { }
        /**
         * Default constructor, make STL happy :)
         */
        cluster_physical_addr() noexcept : id(0)
        { }
    };
    /** replica locator */
    using rloc = cluster_physical_addr;
    /** object locator, i.e. set of locators of ranked replica */
    using oloc = vector<rloc>;

    /**
     * objects that are placed at their calculated location, values of entries are unused
     */
    mutable LRUCache<okey, char, gestalt::defaults::client_locator_cache_size> normal_placements;
    /**
     * caches redirected location of object that are not stored at their default
     * calculated placement, i.e. those require linear search on at least one of
     * its replica.
     * @note currently we treat objects that cannot be placed at their default
     * location as failed insertions, i.e. this cache is currently always empty
     */
    mutable LRUCache<okey, oloc, gestalt::defaults::client_redirection_cache_size> abnormal_placements;
    inline void erase_oloc_cache(const okey &key)
    {
        normal_placements.erase(key);
        abnormal_placements.erase(key);
    }
public:
    /**
     * we store known collisions here, this is only for benchmark
     */
    mutable LRUCache<okey, char, static_cast<size_t>(1e4)> collision_set;

    /* con/dtors */
public:
    Client(const filesystem::path &config_path, unsigned id = 114514);
    /** for now we don't implement HA, cluster map will be static */
    // void refresh_clustermap();

    /* I/O interface */
private:
    /**
     * calculate mapped location
     * @param key object key
     * @param[out] need_search do we still need to search for a justified placement
     * @return ordered set of acting replica location
     */
    oloc map(const okey &key, bool &need_search) const;

    /**
     * Probe for key around all `oloc`s, and justify them to exactly where the
     * object is currently located, or available slots where new object can be
     * inserted. The location will be inserted to locator cache.
     * @note If Client::map(const okey&, bool&) hinted a search is needed, this
     * method must be called, otherwise you will be performing a headless overwrite.
     * @param[in] key object key
     * @param[in,out] ls calculated locators
     * @return 
     * 0 ok, locator can be used
     * -EDQUOT cannot find key nor empty slots
     * -EINVAL found empty slots, available for inserts
     */
    int probe_and_justify_oloc(const okey &key, oloc &ls);

    /**
     * timepoint of last I/O expecting retry, indication of contention
     * @sa gestalt::optimization::retry_holdoff
     */
    decltype(std::chrono::steady_clock::now()) last_retry_tp;
    void maybe_holdoff_retry() const noexcept;

public:
    unique_ptr<ops::Base> read_op;
    /**
     * perform raw read on #key, data will be stored in #read_op.buf
     * @note if calling this variant, validate data on your own
     * @param key 
     * @sa Client::get(const char*)
     */
    int raw_read(const char *key);
    /**
     * perform read on #key
     * @param key 
     * @return validity of read data
     * * 0 ok
     * * -EINVAL data not found
     */
    int get(const char *key);

    unique_ptr<ops::Base> lock_op;
    unique_ptr<ops::Base> unlock_op;
    unique_ptr<ops::Base> write_op;
    /**
     * perform overwrite on #key
     * @note if calling this variant, #write_op must be filled
     * @note currently collision on any replica will be treated as failed
     *      insertion, and -EDQUOT is returned.
     * @return 
     * * 0 ok
     * * -EDQUOT failed to find a slot to fill
     */
    int put(void);
    /**
     * perform write (reset) on #key
     * @param key 
     * @param din 
     * @param dlen 
     * @return 
     * @sa Client::put(void)
     */
    inline int put(const char *key, const void *din, size_t dlen) {
        write_op.get()->buf.set(key, din, dlen);
        return put();
    }

    /**
     * @note currently we don't implement space allocation (reserve) nor
     * revokation (remove), for while our benchmark is running, the working set
     * remains static, that is just how YCSB works.
     */

    /* debug interface */
public:
    inline string dump_clustermap() const
    {
        return node_mapper.dump_clustermap();
    }

};  /* class Client */

}   /* namespace gestalt */
