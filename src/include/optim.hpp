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
 * @note negative optimization, if any difference at all ...
 */
constexpr bool batched_poll = false;

/**
 * Proactively sleep for couple of us on high contention to ease uneffective
 * load on remote RNIC
 * @note Only helpful when contention is high and requests are constantly
 * dropped by remote RNIC (e.g. 48 threads on my one-to-one test deployment).
 * Besides this "optimization" is only heuristic and not guaranteed to solve
 * the problem.
 */
constexpr bool retry_holdoff = false;

}   /* namespace optimization */
}   /* namespace gestalt */
