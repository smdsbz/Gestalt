/**
 * @file params.hpp
 *
 * parameters for fixed data structures
 */

#pragma once

#include "../common/size_literals.hpp"


namespace gestalt {
namespace params {

constexpr size_t hht_search_length = 5;
constexpr size_t data_seg_length = 4_K;
constexpr size_t max_op_size = 1e2 * 4_K + hht_search_length;
constexpr unsigned max_poll_retry = 1e6;
constexpr unsigned eager_retry_threshold_ns = 1e3;

}   /* namespace params */
}   /* namespace gestalt */
