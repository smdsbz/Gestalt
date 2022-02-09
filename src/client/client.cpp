/**
 * @file client.cpp
 */

#include <fstream>

#include <boost/property_tree/ini_parser.hpp>
#include "common/boost_log_helper.hpp"

#include "client.hpp"
#include "internal/data_mapper.hpp"
#include "spec/ops/all.hpp"


namespace gestalt {

using namespace std;


Client::Client(const filesystem::path &config_path) :
    id(/*TODO: random generate, or allocated by monitor*/114514),
    node_mapper(), session_pool(),
    abnormal_placements(gestalt::defaults::client_redirection_cache_size)
{
    {
        ifstream f(config_path);
        boost::property_tree::read_ini(f, config);
    }

    node_mapper = DataMapper(this);
    BOOST_LOG_TRIVIAL(debug) << "DataMapper initialized: "
        << node_mapper.dump_clustermap();

    session_pool = RDMAConnectionPool(this);
    BOOST_LOG_TRIVIAL(debug) << "RDMAConnectionPool initialized";

    // TODO: initialized structured RDMA ops
}

}   /* namespace gestalt */
