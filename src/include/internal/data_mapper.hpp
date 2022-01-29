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
#include <unordered_map>
#include <sstream>

#include "spec/dataslot.hpp"


namespace gestalt {

using namespace std;

using okey = dataslot::key_type;
class Client;
class RDMAConnectionPool;


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
        server_node &operator=(const server_node &other) = default;
    };
    /** map of candicate servers for `client`'s bucket, server ID -> property */
    unordered_map<unsigned, server_node> server_map;
    /**
     * rank of server, same of that calculated and returned by monitor on a
     * given bucket
     * @note we actually only have one bucket now
     */
    vector<unsigned> server_rank;
public:
    /**
     * type of DataMapper calculated output, which is just an array of server ID
     */
    using acting_set = vector<unsigned>;
    friend class gestalt::RDMAConnectionPool;

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
    acting_set map(const string &k, unsigned r) const;
    void mark_out(unsigned id);

    inline string dump_clustermap() const
    {
        ostringstream os;
        os << "[";
        for (unsigned r = 0; r < server_rank.size(); ++r) {
            const auto id = server_rank[r];
            const auto &s = server_map.at(id);
            os << "Server("
                << "id=" << id << ", ";
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
