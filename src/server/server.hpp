/**
 * @file server.hpp
 */

#pragma once

#include <filesystem>
#include <memory>
#include <functional>

#include <boost/property_tree/ini_parser.hpp>
#include <boost/core/noncopyable.hpp>
#include <boost/asio/ip/address.hpp>
#include <rdma/rdma_cma.h>

#include "headless_hashtable.hpp"
#include "spec/dataslot.hpp"


namespace gestalt {
using namespace std;

/**
 * Server runtime
 */
class Server final : boost::noncopyable {

    /* cluster runtime */
    const unsigned id;  ///< server unique ID

    /* instance runtime */
    const boost::property_tree::ptree config;   ///< configurations

    /* storage management */
    /**
     * managed PMem space, must supply a custom deleter that calls pmem_unmap()
     */
    unique_ptr<void, std::function<void(void*)>> pmem_space;
    size_t pmem_size;
    HeadlessHashTable<dataslot> storage;    ///< storage container

    /* network management */
    const boost::asio::ip::address addr;    ///< server network interface

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
        filesystem::path dax_path);
    /**
     * Don't use this directly, use create() instead
     * @private
     */
    Server(
        unsigned _id,
        const boost::property_tree::ptree &_cfg,
        unique_ptr<void> &&_pmem, size_t _size,
        const boost::asio::ip::address &_addr
    ) noexcept :
        id(_id), config(_cfg),
        pmem_space(std::move(_pmem)), pmem_size(_size),
        storage(static_cast<dataslot*>(pmem_space.get()), pmem_size / sizeof(dataslot)),
        addr(_addr)
    { }

    ~Server();

};  /* class Server */

}   /* namespace gestalt */
