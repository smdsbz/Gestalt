/**
 * @file server.hpp
 */

#pragma once

#include <filesystem>
#include <memory>

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
    unsigned id;    ///< server unique ID

    /* instance runtime */
    boost::property_tree::ptree config;     ///< configurations

    /* storage management */
    unique_ptr<HeadlessHashTable<dataslot>> storage;    ///< storage container

    /* network management */
    boost::asio::ip::address addr;  ///< server network interface

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
        unsigned id, const string &addr);
    /**
     * @private
     * @param _id 
     * @param _cfg 
     * @param _s 
     * @param _addr 
     */
    Server(
        unsigned _id,
        const boost::property_tree::ptree &_cfg,
        HeadlessHashTable<dataslot> *_s,
        const boost::asio::ip::address &_addr
    ) noexcept :
        id(_id), config(_cfg), storage(_s), addr(_addr)
    { }

    ~Server();

};  /* class Server */

}   /* namespace gestalt */
