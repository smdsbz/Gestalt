/**
 * @file dataslot.hpp
 *
 * Data structure layout of a slot in headless hashtable.
 *
 * Algorithms are also included in this header file, for in a completely client-
 * centric distributed system, all calculations should be idempotent, and all
 * verifications should follow the same protocal.
 */

#pragma once

#include <cstdint>
#include <concepts>
#include <atomic>
#include <cstring>
#include <stdexcept>
#include <isa-l/crc.h>
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
 *      +-----------------------------------+
 *   0  |                                   |
 *      |                Key                |
 *      |                                   |
 *      +-----------------------------------+
 * 496+  0        4        8                 16
 *      +--------+--------+-----------------+
 *      | Length | D. CRC |  Atomic Region  |
 *      +--------+--------+-----------------+
 * 512B
 * ```
 *
 * Where `D. CRC` stands for CRC of the user data in the current slot.
 *
 * An empty key (or starts with '\0') should indicate an invalid slot.
 *
 * If length is larger than `DATA_SEG_LEN`, the KV entry spans accross multiple
 * consecutive slots. Only the first segment records length of the entire entry.
 *
 * __Atomic Region__
 *
 * ```text
 *  0        8        16       24       32       40       48       56       64b
 * +-----------------------------------+--------------------------+--------+
 * |          Key Hash (CRC)           |                          |V      L|
 * +-----------------------------------+--------------------------+--------+
 * ```
 *
 * Where `V` is valid bit, `L` is lock bit.
 *
 * According to RDMA specification, Writes are performed sequencially, therefore
 * placing lock bit at the very end of a slot allows us to update data and then
 * immediately unlock the slot in one RDMA Write operation, and that unlock is
 * guaranteed to happen after the update.
 *
 * A key hash (or rather, fingerprint) is included so we can perform headless
 * CAS on wanted KV, without having to compare the entire key, possibly introducing
 * another Read and breaking atomicity.
 *
 * Unspecified fields should be zeroed, as the whole 64 bit atomic region is to
 * be CAS-ed.
 */
struct [[gnu::packed]] dataslot_meta {
    /**
     * Packed C-style string, with handy helpers
     */
    struct [[gnu::packed]] key_type {
        char _k[496];

        /* constructors */

        /**
         * Default empty constructor, constructs an invalid slot metadata.
         */
        key_type() noexcept
        {
            _k[0] = '\0';
        }
        key_type(const char *k)
        {
            this->set(k);
        }
        key_type(const string &k)
        {
            this->set(k.c_str());
        }
        key_type &operator=(const key_type &that) noexcept
        {
            strcpy(this->_k, that._k);
            return *this;
        }

        /* required interfaces */
    public:
        inline const auto c_str() const noexcept
        {
            return _k;
        }
        inline bool operator==(const key_type &that) const noexcept
        {
            return !strcmp(this->_k, that._k);
        }
        inline bool operator!=(const key_type &that) const noexcept
        {
            return !(*this == that);
        }

        static inline uint32_t hash(const string &k) noexcept
        {
            return crc32_iscsi((unsigned char*)k.c_str(), k.length(), 0x114514);
        }
        inline auto hash() const
        {
            return hash(_k);
        }

        /* additional helpers */
    public:
        inline void set(const char *k)
        {
            if (strlen(k) > sizeof(key))
                throw std::invalid_argument("key too long");
            strcpy(this->_k, k);
        }
        inline bool valid() const noexcept
        {
            return !!_k[0];
        }
        inline void invalidate() noexcept
        {
            _k[0] = '\0';
        }
    } key;

    uint32_t length;
    uint32_t data_crc;

    enum bits_shift {
        lock,
        valid = 7,
    };
    enum bits_flag : uint8_t {
        none = 0,
        lock = 1 << bits_shift::lock,
        valid = 1 << bits_shift::valid,
    };
    union {
        uint64_t u64;
        struct [[gnu::packed]] {
            uint32_t key_crc;
            uint8_t _[3];
            atomic<bits_flag> bits;
        } m;
    } atomic;

    /* constructors */
public:
    /**
     * Default constructor, constructs invalid slot metadata.
     */
    dataslot_meta() noexcept : key(), atomic({.m.bits = bits_flag::none}) {}
    dataslot_meta(const char *k) :
        key(k), atomic({.m = {.key_crc = key.hash(), .bits = bits_flag::valid}})
    { }
    /**
     * Helper when initialized with data.
     * @param k key
     * @param dlen data length
     * @param dcrc data CRC
     */
    dataslot_meta(const char *k, unsigned dlen, uint32_t dcrc) :
        key(k), length(dlen), data_crc(dcrc),
        atomic({.m = {.key_crc = key.hash(), .bits = bits_flag::valid}})
    { }

    /* helpers */
public:
    /**
     * Checks key-related fields.
     * @return
     * * 0 if key-related fields are valid
     * * -EINVAL if key is invalid, i.e. slot is unused
     * * -ECOMM if key CRC does not match
     */
    inline int key_valid() const noexcept
    {
        if (!key.valid() || !(atomic.m.bits & bits_flag::valid))
            return -EINVAL;
        if (key.hash() != atomic.m.key_crc)
            return -ECOMM;
        return 0;
    }
    inline void invalidate() noexcept
    {
        atomic.m.bits = bits_flag::none;
        key.invalidate();
    }
    /**
     * Sets key-related field in metadata
     *
     * The lock bit is clear by default.
     *
     * @param k new key
     */
    inline void set_key(const char *k)
    {
        key.set(k);
        atomic.m.key_crc = key.hash();
        atomic.m.bits = bits_flag::valid;
    }
};
static_assert(std::is_standard_layout_v<dataslot_meta>);
static_assert(sizeof(dataslot_meta::atomic) == 8);
static_assert(sizeof(dataslot_meta) == 512_B);


struct dataslot {
    using meta_type = dataslot_meta;
    using key_type = dataslot_meta::key_type;
    using value_type = uint8_t[DATA_SEG_LEN];

    value_type data;
    meta_type meta;

    /* constructors */
public:

    /* required interface */
public:

    /* helpers */
public:

};
static_assert(std::is_standard_layout_v<dataslot>);
static_assert(sizeof(dataslot) % 512_B == 0);

}   /* namespace gestalt */
