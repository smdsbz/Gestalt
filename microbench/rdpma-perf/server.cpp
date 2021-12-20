/**
 * @file server.cpp
 *
 * Server-side (target instance) of our perf test
 */

#include <boost/program_options.hpp>
#include <iostream>
#include <filesystem>
#include <stdexcept>
#include <libpmem.h>
#include <rdma/rdma_cma.h>
#include "misc/numa.hpp"
#include "misc/ddio.hpp"
#include "common/defer.hpp"
#include "common/size_literals.hpp"

using namespace std;


int main(const int argc, const char **argv)
{
    /* program arguments */
    std::filesystem::path pmem_file_path;

    namespace po = boost::program_options;
    po::options_description opt_desc;
    opt_desc.add_options()
        ("pmem-file", po::value(&pmem_file_path), "Path to DAX file providing PMem space.");
    po::variables_map opt_map;
    po::store(po::parse_command_line(argc, argv, opt_desc), opt_map);
    po::notify(opt_map);

    /* map PMem space */
    if (!filesystem::exists(pmem_file_path) || !filesystem::is_regular_file(pmem_file_path))
        throw std::invalid_argument("pmem-file");

    size_t pmem_buffer_size; int is_pmem;
    auto pmem_buffer = pmem_map_file(
        pmem_file_path.c_str(), 0, PMEM_FILE_EXCL, 0,
        &pmem_buffer_size, &is_pmem
    );
    if (!pmem_buffer)
        throw std::runtime_error(string("pmem_map_file: ") + std::strerror(errno));
    defer([&] { pmem_unmap(pmem_buffer, pmem_buffer_size); });
    std::cout << "size of mapped PMem file is " << to_human_readable(pmem_buffer_size) << std::endl;



    /* TODO: init RNIC */

    /* TODO: register PMem to RNIC to be RW-ready */

    /* TODO: halt until interrupt */

    return 0;
}
