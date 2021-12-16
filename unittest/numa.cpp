/**
 * @file numa.cpp
 * Unittest for misc/numa
 */

#define BOOST_TEST_MODULE gestalt misc numa
#include <boost/test/unit_test.hpp>
#include <boost/log/trivial.hpp>
#include <fstream>
#include <filesystem>
#include "misc/numa.hpp"
#include "common/size_literals.hpp"
#include "common/defer.hpp"

using namespace std;

const char *PMEM_DEV = "pmem1";
const auto PMEM_FS = filesystem::path("/data") / PMEM_DEV;
const auto TEST_FILE = filesystem::path(PMEM_FS) / "gestalt_test.img";
const size_t TEST_FILE_SIZE = 10_K;


BOOST_AUTO_TEST_CASE(test_choose_rnic_on_same_numa) {
    using namespace gestalt::misc::numa;

    if (!filesystem::exists(PMEM_FS) || !filesystem::is_directory(PMEM_FS))
        BOOST_TEST_REQUIRE(false, "no PMem FS mounted at " << PMEM_FS
            << ", test abort");

    /* get PMem space */
    {
        ofstream f(TEST_FILE, std::ios::binary);
        f.seekp(TEST_FILE_SIZE);
    }

    int num_devices;
    auto devices = rdma_get_devices(&num_devices);
    BOOST_REQUIRE(devices);
    defer(devices, rdma_free_devices);
    auto choice = choose_rnic_on_same_numa(PMEM_DEV, devices, num_devices);
    BOOST_TEST_REQUIRE(choice,
        "failed to choose RNIC for PMem device " << PMEM_DEV
        << ", this might be expected due to lack of actual RNIC hardware.");
    BOOST_LOG_TRIVIAL(info) << "test_choose_rnic_on_same_numa: "
        << "chose " << choice->name << " for device " << PMEM_DEV;

    filesystem::remove(TEST_FILE);
}
