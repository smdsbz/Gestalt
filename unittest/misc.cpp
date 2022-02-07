/**
 * @file numa.cpp
 * Unittest for misc/numa
 */

#define BOOST_TEST_MODULE gestalt misc numa
#include <boost/test/unit_test.hpp>
#include <boost/log/trivial.hpp>
#include <cstdlib>
#include <fstream>
#include <filesystem>
#include "misc/numa.hpp"
#include "misc/ddio.hpp"
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
    defer([&]{ filesystem::remove(TEST_FILE); });

    int num_devices;
    auto devices = rdma_get_devices(&num_devices);
    BOOST_REQUIRE(devices);
    defer([&]{ rdma_free_devices(devices); });
    auto choice = choose_rnic_on_same_numa(PMEM_DEV, devices);
    BOOST_TEST_REQUIRE(choice,
        "failed to choose RNIC for PMem device " << PMEM_DEV
        << ", this might be expected due to lack of actual RNIC hardware.");
    BOOST_LOG_TRIVIAL(info) << "test_choose_rnic_on_same_numa: "
        << "chose " << choice->device->name << " for device " << PMEM_DEV;
}


BOOST_AUTO_TEST_CASE(test_ddio_guard) {
    using namespace gestalt::misc;
    const char *TMPFILE = "/tmp/gestalt/setpci.out";
    const char *RNIC = "mlx5_0";
    const char *PCI_ROOT = "0000:ae:00.0";

    {
        auto guard(ddio::scope_guard::from_rnic(RNIC));
        {
            std::system((string("setpci -s ") + PCI_ROOT + " 180.b > " + TMPFILE).c_str());
            ifstream f(TMPFILE);
            int r; f >> std::hex >> r;
            BOOST_TEST_REQUIRE(r == 0x11, "it could be the code not working, or"
                << " misconfigured hardware");
            filesystem::remove(TMPFILE);
        }
    }

    {
        std::system((string("setpci -s ") + PCI_ROOT + " 180.b > " + TMPFILE).c_str());
        ifstream f(TMPFILE);
        int r; f >> std::hex >> r;
        BOOST_TEST_REQUIRE(r == 0x91, "it could be the code not working, or"
            << " misconfigured hardware");
        filesystem::remove(TMPFILE);
    }
}
