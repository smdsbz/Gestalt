/**
 * @file client.hpp
 *
 * @note All HA features not implemented.
 * @see src/server/server.hpp
 */

#pragma once

#include <filesystem>
#include <unordered_map>

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
    /** pooled RDMA connection */
    RDMAConnectionPool session_pool;
    friend class RDMAConnectionPool;

    /* misc */

    struct cluster_physical_addr {
        /** server ID */
        unsigned id;
        /** redirected VA on that server */
        uintptr_t addr;
    public:
        cluster_physical_addr(unsigned _id, uintptr_t _addr) noexcept :
            id(_id), addr(_addr)
        { }
        /**
         * Default constructor, make STL happy :)
         */
        cluster_physical_addr() noexcept : id(0)
        { }
    };
    /**
     * caches redirected location of objects that are not stored at their default
     * calculated location
     */
    LRUCache<okey, cluster_physical_addr> abnormal_placements;

    /* con/dtors */
public:
    Client(const filesystem::path &config_path);
    /** for now we don't implement HA */
    // void refresh_clustermap();

    /* I/O interface */
public:
    unique_ptr<ops::Base> read_op;
    void read(const char *key);

    /* debug interface */
public:
    inline string dump_clustermap() const
    {
        return node_mapper.dump_clustermap();
    }

};  /* class Client */

}   /* namespace gestalt */
