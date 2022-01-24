/**
 * @file server.hpp
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
class Server final : boost::noncopyable {

    /* cluster runtime */

    /** server instance unique ID */
    const unsigned id;

    /* instance runtime */

    const boost::property_tree::ptree config;

    /* storage management */

    /** managed PMem space, supplied to storage */
    struct managed_pmem_t : boost::noncopyable {
        void* buffer;
        size_t size;
    public:
        managed_pmem_t(void *_b, size_t _s) noexcept : buffer(_b), size(_s)
        { }
        managed_pmem_t(managed_pmem_t &&o) noexcept : buffer(o.buffer), size(o.size)
        { }
        ~managed_pmem_t()
        {
            pmem_unmap(buffer, size);
        }
    } managed_pmem;
    /** storage container */
    HeadlessHashTable<dataslot> storage;

    /* network management */

    /** server network (both RPC and RDMA) interface */
    const boost::asio::ip::address addr;
    /** name of the RNIC to use, currently this is not verified! */
    const string rnic_name;

    struct __RdmaEpDeleter {
        inline void operator()(rdma_cm_id *ep)
        {
            rdma_destroy_ep(ep);
        }
    };
    /** the RDMA endpoint listening for incoming RDMA connections */
    unique_ptr<rdma_cm_id, __RdmaEpDeleter> listen_id;

    struct __RdmaConnDeleter {
        inline void operator()(rdma_cm_id *ep)
        {
            if (int r = rdma_disconnect(ep); r)
                throw std::runtime_error(string("rdma_disconnect(): ") + strerror(errno));
            rdma_destroy_ep(ep);
        }
    };
    /** client unique ID -> accepted connection endpoint */
    unordered_map<
        unsigned,
        unique_ptr<rdma_cm_id, __RdmaConnDeleter>
    > connected_client_id;

    /* runtime */

    misc::ddio::scope_guard ddio_guard;
    atomic<bool> is_stopping;
    mutex _mutex;

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
        const string &_rnic,
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
