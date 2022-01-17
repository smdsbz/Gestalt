/**
 * @file dataslot.hpp
 *
 * Data structure layout of a slot in headless hashtable.
 */

#pragma once

#include <cstdint>
#include <concepts>
#include "../common/size_literals.hpp"


namespace gestalt {

using namespace std;

/* CMBK: tune this! */
constexpr size_t DATA_SEG_LEN = 4_K;

/**
 * Per-slot Metadata Blob
 *
 * __Overview__
 *
 * ```text
 *      +---------------+
 *   0  |               |
 *  64  |               |
 * 128  |      Key      |
 * 192  |               |
 * 256  |               |
 *      +---------------+
 * 320  |    Length     |
 *      +---------------+
 * 384  |   Data CRC    |
 *      +---------------+
 * 448  |  Atomic Reg.  |
 *      +---------------+
 * 512
 * ```
 *
 * __Atomic Region__
 *
 * TODO:
 *
 * ```text
 * 448+  0        8        16       24       32       40       48       56     63
 *      +--------+--------+--------+--------+--------+--------+--------+--------+
 *      |        |        |        |        |        |        |        |       L|
 *      +--------+--------+--------+--------+--------+--------+--------+--------+
 * 512
 * ```
 *
 * Where `L` is lock bit.
 *
 * According to RDMA specification, Writes are performed sequencially, therefore
 * placing lock bit at the end allows us to update data and then immediately
 * unlock the slot in one RDMA Write operation, and that unlock is guaranteed to
 * happen after the update.
 */
[[gnu::packed]]
struct dataslot_meta {
    // TODO:
    char key[320];
    uint64_t length;
};
static_assert(std::is_standard_layout_v<dataslot_meta>);
static_assert(sizeof(dataslot_meta) == 512_B);

struct dataslot {
    using meta_type = dataslot_meta;

    uint8_t data[DATA_SEG_LEN];
    meta_type meta;
};
static_assert(std::is_standard_layout_v<dataslot>);
static_assert(sizeof(dataslot) % 512_B == 0);

}   /* namespace gestalt */
