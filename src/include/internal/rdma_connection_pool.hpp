/**
 * @file rdma_connection_pool.hpp
 */

#pragma once

#include <unordered_map>
#include <filesystem>
#include <memory>
#include <arpa/inet.h>

#include "common/boost_log_helper.hpp"

#include <rdma/rdma_cma.h>

#include "spec/dataslot.hpp"


namespace gestalt {

using namespace std;

using okey = dataslot::key_type;
class Client;


class RDMAConnectionPool {
    Client *client;
    friend class gestalt::Client;

    struct __RdmaConnDeleter {
        inline void operator()(rdma_cm_id *ep)
        {
            if (rdma_disconnect(ep))
                boost_log_errno_throw(rdma_disconnect);
            rdma_destroy_ep(ep);
            BOOST_LOG_TRIVIAL(debug) << "RDMA disconnected from "
                << inet_ntoa(ep->route.addr.dst_sin.sin_addr);
        }
    };
    struct memory_region {
        /** VA on remote */
        uintptr_t addr;
        size_t length;
        size_t slots;
        uint32_t rkey;
        /** RDMA connection */
        unique_ptr<rdma_cm_id, __RdmaConnDeleter> conn;
    public:
        memory_region() noexcept : length(0)
        { }
        memory_region(
                uintptr_t _addr, size_t _len, uint32_t _rkey,
                decltype(conn) &&_conn) noexcept :
            addr(_addr), length(_len), slots(length / DATA_SEG_LEN), rkey(_rkey),
            conn(std::move(_conn))
        { }
        memory_region(memory_region &&tmp) = default;
        memory_region &operator=(memory_region &&tmp) = default;
        ~memory_region()
        {
            BOOST_LOG_TRIVIAL(debug) << "RDMA disconnecting from "
                << inet_ntoa(conn->route.addr.dst_sin.sin_addr);
        }
    };
    /** session pool, server ID -> MR fields */
    unordered_map<unsigned, memory_region> pool;

    /* c/dtors */
public:
    /**
     * initializer, RDMAConnectionPool is move-constructed
     * @param _c 
     */
    explicit RDMAConnectionPool(Client *_c);
    RDMAConnectionPool() noexcept : client(nullptr)
    { }
    RDMAConnectionPool &operator=(RDMAConnectionPool &&tmp) = default;
    ~RDMAConnectionPool()
    { }

    /* interface */
public:
    // TODO: register_op()

};  /* class RDMAConnection Pool */

}   /* namespace gestalt */
