/**
 * @file client.cpp
 */

#include <fstream>

#include <boost/property_tree/ini_parser.hpp>
#include "common/boost_log_helper.hpp"

#include "client.hpp"
#include "./data_mapper.hpp"


namespace gestalt {

using namespace std;


Client::Client(const filesystem::path &config_path) :
    node_mapper(), fibers()
{
    {
        ifstream f(config_path);
        boost::property_tree::read_ini(f, config);
    }

    node_mapper = DataMapper(this);

    // TODO:
}

}   /* namespace gestalt */
