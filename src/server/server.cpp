/**
 * @file server.cpp
 *
 * Implementation of the server instance.
 */

#include <filesystem>
#include <fstream>
#include <libpmem.h>
#include <rdma/rdma_cma.h>
#include <boost/log/trivial.hpp>
#include "spec/dataslot.hpp"
#include "headless_hashtable.hpp"
#include "misc/numa.hpp"
#include "misc/ddio.hpp"
#include "common/size_literals.hpp"
#include "common/defer.hpp"

using namespace std;
