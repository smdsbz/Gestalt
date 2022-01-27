/**
 * @file data_mapper.cpp
 *
 * DataMapper - mapping object key to server nodes in a calculated fashion
 *
 * For now we may only test on limited number of server nodes, data mapping
 * among server nodes is done in a round-robin fashion.
 *
 * Final mapping to RDPMA VA is done inside class RDMAConnectionPool .
 */

#pragma once

#include <string>
#include <vector>
#include <sstream>

#include "spec/dataslot.hpp"


namespace gestalt {

using namespace std;

using okey = dataslot::key_type;
class Client;


class DataMapper {
    Client *client;
    friend class gestalt::Client;

    struct server_node {
        /**
         * NOTE: HA feature not implemented, not used
         */
        enum class Status {
            in, up, out,
        } status;
        /** server IP address */
        string addr;
    public:
        server_node() noexcept : status(Status::out)
        { }
        server_node(const string &_addr) noexcept :
            status(Status::up), addr(_addr)
        { }
        server_node(const server_node &other) = default;
    };
    /**
     * pool of servers, with index being server ID
     * @note [0] is always out, since 0 is an invalid server ID
     */
    vector<server_node> server_list;
public:
    /**
     * type of DataMapper calculated output, where elements are internal indices
     * to `server_list`
     */
    using acting_set = vector<unsigned>;
    // friend class gestalt::RDMAConnectionPool;

    /* con/dtors */
public:
    /**
     * initializer, DataMapper is move-constructed
     * @param _c parent gestalt::Client
     */
    explicit DataMapper(Client *_c);
    DataMapper() noexcept : client(nullptr)
    { }
    DataMapper &operator=(DataMapper &&tmp) = default;
    ~DataMapper()
    { }

    /* interface */
public:
    /**
     * Get server nodes responsible for some object key
     *
     * Simple linear-probe.
     *
     * @param k object key (okey)
     * @param r replica count
     * @return ordered acting set of size `r`, if smaller than `r` then something
     * is wrong.
     */
    acting_set map(const string &k, unsigned r);

    inline string dump_clustermap() const
    {
        ostringstream os;
        os << "[";
        for (unsigned i = 0; i < server_list.size(); ++i) {
            const auto &s = server_list[i];
            os << "Server("
                << "id=" << i << ", ";
            os << "status=";
            switch (s.status) {
            case server_node::Status::in:
                os << "in";
                break;
            case server_node::Status::out:
                os << "out";
                break;
            case server_node::Status::up:
                os << "up";
                break;
            default:
                os << "unknown";
                break;
            }
            os << ", ";
            os << "addr=" << s.addr
                << "), ";
        }
        os << "]";
        return os.str();
    }

};  /* class DataMapper */

}   /* namespace gestalt */
