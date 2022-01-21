/**
 * @file server.hpp
 */

#pragma once

#include <filesystem>
#include <memory>

#include <boost/property_tree/ini_parser.hpp>
#include <boost/core/noncopyable.hpp>
#include <rdma/rdma_cma.h>

#include "headless_hashtable.hpp"
#include "spec/dataslot.hpp"


namespace gestalt {
using namespace std;

/**
 * Server runtime
 */
class Server : boost::noncopyable {

    /* cluster runtime */
    int id;     ///< server unique ID

    /* instance runtime */
    boost::property_tree::ptree config;     ///< configurations

    /* storage management */
    unique_ptr<HeadlessHashTable<dataslot>> storage;    ///< storage container

    /* network management */


    /* con/destructors */
public:
    /**
     * Server runtime factory
     * @param config_path path to gestalt.conf
     * @return Server instance
     */
    static unique_ptr<Server> create(const filesystem::path &config_path);
    ~Server();
private:
    Server(
        int _id,
        const boost::property_tree::ptree &_cfg,
        HeadlessHashTable<dataslot> *_s
    ) noexcept :
        id(_id), config(_cfg), storage(_s)
    { }


};  /* class Server */

}   /* namespace gestalt */
