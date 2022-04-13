/**
 * @file
 *
 * Optimization switches
 */

#pragma once


namespace gestalt {
namespace optimization {

/**
 * Share RDMA Completion Queue and poll them in batches, merging PCIe requests
 * to RNIC
 */
constexpr bool batched_poll = true;

/**
 * Proactively sleep for couple of us on high contention to ease uneffective
 * load on remote RNIC
 */
constexpr bool retry_holdoff = true;

}   /* namespace optimization */
}   /* namespace gestalt */
