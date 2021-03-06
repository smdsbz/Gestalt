/**
 * @file server.hpp
 *
 * @note This build is only meant for performance benchmarking, all HA features
 * are not implemented, and there is only one bucket (key space) that maps to the
 * entire mapped PMem.
 */

#pragma once

#include <filesystem>
#include <memory>
#include <functional>
#include <unordered_map>
#include <atomic>
#include <mutex>

#include <boost/property_tree/ini_parser.hpp>
#include <boost/core/noncopyable.hpp>
#include <boost/asio/ip/address.hpp>
#include "common/boost_log_helper.hpp"
#include <libpmem.h>
#include <rdma/rdma_cma.h>

#include "headless_hashtable.hpp"
#include "misc/ddio.hpp"
#include "spec/dataslot.hpp"


namespace gestalt {
using namespace std;

namespace rpc {
class SessionServicer;
}

/**
 * Server runtime
 */
class Server final : private boost::noncopyable {

    /* cluster runtime */

    /** server instance unique ID */
    const unsigned id;

    /* instance runtime */

    const boost::property_tree::ptree config;

    /* storage management */

    /** managed PMem space, supplied to storage */
    struct managed_pmem_t : private boost::noncopyable {
        void* buffer;
        size_t size;
    public:
        managed_pmem_t(void *_b, size_t _s) noexcept : buffer(_b), size(_s)
        { }
        managed_pmem_t(managed_pmem_t &&o) noexcept : buffer(o.buffer), size(o.size)
        {
            o.buffer = nullptr;
        }
        ~managed_pmem_t()
        {
            if (!buffer)
                return;
            pmem_unmap(buffer, size);
        }
    } managed_pmem;
    /** storage container */
    HeadlessHashTable<dataslot> storage;

    /* network management */

    /** server network (both RPC and RDMA) interface */
    const boost::asio::ip::address addr;
    /** global libibverbs context, i.e. list of devices */
    struct managed_ibvctx_t : private boost::noncopyable {
        /** null-terminated array */
        ibv_context **devices;
        ibv_context *chosen;
    public:
        managed_ibvctx_t(ibv_context **_devs) noexcept :
            devices(_devs), chosen(NULL)
        { }
        managed_ibvctx_t(managed_ibvctx_t &&o) noexcept :
            devices(o.devices), chosen(o.chosen)
        {
            o.devices = NULL;
        }
        ~managed_ibvctx_t()
        {
            if (!devices)
                return;
            rdma_free_devices(devices);
        }
    } ibvctx;
    struct __IbvMrDeleter {
        inline void operator()(ibv_mr *mr)
        {
            if (errno = ibv_dereg_mr(mr); errno)
                boost_log_errno_throw(ibv_dereg_mr);
        }
    };
    /** memory region registered to #ibvctx */
    unique_ptr<ibv_mr, __IbvMrDeleter> ibvmr;
    struct __RdmaListenEpDeleter {
        inline void operator()(rdma_cm_id *ep)
        {
            if (errno = ibv_dealloc_pd(ep->pd); errno)
                boost_log_errno_throw(ibv_dealloc_pd);
            rdma_destroy_ep(ep);
        }
    };
    /**
     * the RDMA endpoint listening for incoming RDMA connections, should have
     * an associated PD.
     */
    unique_ptr<rdma_cm_id, __RdmaListenEpDeleter> listen_id;

    struct __RdmaConnDeleter {
        inline void operator()(rdma_cm_id *ep)
        {
            if (int r = rdma_disconnect(ep); r)
                boost_log_errno_throw(rdma_disconnect);
            rdma_destroy_ep(ep);
        }
    };
    struct client_prop_t : private boost::noncopyable {
        unique_ptr<rdma_cm_id, __RdmaConnDeleter> ep;
    public:
        client_prop_t(decltype(ep) &&_ep) noexcept :
            ep(std::move(_ep))
        { }
        client_prop_t(client_prop_t &&o) noexcept :
            ep(std::move(o.ep))
        { }
    };
    /** client ID -> accepted connection endpoint */
    unordered_map<unsigned, client_prop_t> connected_client_id;

    /* runtime */

    misc::ddio::scope_guard ddio_guard;
    atomic<bool> is_stopping;
    mutex _mutex;

    struct bucket_descriptor {
        uintptr_t addr;
        size_t length;
    };
    /**
     * buckets that this server is responsible for
     * @todo currently we only have one big bucket, so this is not used.
     */
    unordered_map<string, bucket_descriptor> buckets;

    /* con/destructors */
public:
    /**
     * Server runtime factory
     * @param config_path path to gestalt.conf
     * @param id server ID, if 0 let monitor generate new one
     * @param addr server address
     * @return Server instance
     * @throw std::runtime_error
     */
    static unique_ptr<Server> create(
        const filesystem::path &config_path,
        unsigned id, const string &addr,
        const filesystem::path &dax_path);
    /**
     * Don't use this directly, use create() instead
     * @private
     */
    Server(
        unsigned _id,
        const boost::property_tree::ptree &_cfg,
        managed_pmem_t &&_pmem,
        const boost::asio::ip::address &_addr,
        decltype(ibvctx) &&_ibvctx, decltype(ibvmr) &&_ibvmr,
        decltype(listen_id) &&_listen_id);
    ~Server();

    /* interface */
public:
    /**
     * runs indefinitely unless stop() called
     * @note call this only once, and call this before stop()
     */
    void run();
    friend class gestalt::rpc::SessionServicer;
    /**
     * signals run() to stop
     * @note call this only once, and call this after run()
     */
    void stop();

};  /* class Server */

}   /* namespace gestalt */
