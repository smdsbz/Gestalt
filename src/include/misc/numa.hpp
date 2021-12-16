/**
 * @file numa.hpp
 * Handy helpers for NUMA control.
 */

#pragma once

#include <rdma/rdma_verbs.h>

namespace gestalt {
namespace misc {
namespace numa {

/**
 * Get NUMA node of the RNIC
 *
 * @param dev RNIC device
 * @return NUMA ID, or -1 on cannot detect (e.g. RXE)
 */
int get_numa_node(const ibv_device *dev);

/**
 * Get an RNIC on the same NUMA as PMem device (or namespace)
 *
 * CMBK: The current implementation returns the first satisfying RNIC in the
 * list, no load balancing algorithm is implemented.
 *
 * @param pmem_dev PMem system device name, e.g. "pmem1"
 * @param devices RNICs to choose from
 * @param num_devices length of `devices`
 * @return RNIC device struct, nullptr on no match
 * @throw std::runtime_error no `pmem_dev` found in topology
 */
ibv_device *choose_rnic_on_same_numa(
    const char *pmem_dev,
    ibv_context **devices, size_t num_devices
);

}   /* namespace numa */
}   /* namespace misc */
}   /* namespace gestalt */
