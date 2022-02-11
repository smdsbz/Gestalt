/**
 * @file client.cpp
 */

#include <fstream>

#include <boost/property_tree/ini_parser.hpp>
#include "common/boost_log_helper.hpp"

#include "client.hpp"
#include "internal/data_mapper.hpp"
#include "./ops/all.hpp"


namespace gestalt {

using namespace std;

using ReadOp = ops::Read;


Client::Client(const filesystem::path &config_path) :
    id(/*TODO: random generate, or allocated by monitor*/114514),
    node_mapper(), ibvctx(), session_pool(),
    abnormal_placements(gestalt::defaults::client_redirection_cache_size)
{
    {
        ifstream f(config_path);
        boost::property_tree::read_ini(f, config);
    }

    node_mapper = DataMapper(this);
    BOOST_LOG_TRIVIAL(debug) << "DataMapper initialized: "
        << node_mapper.dump_clustermap();

    {
        ibvctx.devices = rdma_get_devices(NULL);
        if (!ibvctx.devices) {
            BOOST_LOG_TRIVIAL(fatal) << "No RNIC found!";
            throw std::runtime_error("no RNIC");
        }
        ibvctx.chosen = ibvctx.devices[0];

        ibvpd.reset(ibv_alloc_pd(ibvctx.chosen));
        if (!ibvpd)
            boost_log_errno_throw(ibv_alloc_pd);
    }

    session_pool = RDMAConnectionPool(this);
    BOOST_LOG_TRIVIAL(debug) << "RDMAConnectionPool initialized";

    /* initialize structured RDMA ops */
    read_op.reset(new ReadOp(ibvpd.get()));
}

}   /* namespace gestalt */
