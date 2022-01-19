/**
 * @file dataslot.hpp
 *
 * Data structure layout of a slot in headless hashtable.
 *
 * Algorithms are also included in this header file, for in a completely client-
 * centric distributed system, all calculations should be idempotent, and all
 * verifications should follow the same protocal.
 *
 * @note
 * Implementation in this header file is intended to be used by clients, and all
 * memory operations are (or at least, could be) temporal, persistency is not
 * guaranteed! Fast local PMem manipulation support at server side is currently
 * not listed on the roadmap, as this implementation is likely to be only used
 * for benchmarking, and full production feature (like high availability recovery
 * procedure, scrubbing) will remain conceptual :)
 */

#define USING_PMEM false
#ifndef USING_PMEM
#error "USING_PMEM must be specified before #include-ing"
#endif
static_assert(!USING_PMEM);

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
            return crc32_iscsi((uint8_t*)k.c_str(), k.length(), 0x114514);
        }
        inline auto hash() const
        {
            return hash(_k);
        }

        /* additional helpers */
    public:
        inline void set(const char *k)
        {
            if (strlen(k) > sizeof(key) - 1)
                throw std::invalid_argument("key too long");
            strcpy(this->_k, k);
        }
        inline bool is_valid() const noexcept
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
    [[deprecated]]
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
     * * -ECOMM if key is set but CRC does not match
     */
    inline int key_validity() const noexcept
    {
        if (!key.is_valid())
            return -EINVAL;
        if (!(atomic.m.bits & bits_flag::valid))
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

    /**
     * Check if metadata indicates the slot is currently (write) locked.
     *
     * @note
     * This method only checks bits and nothing else, one should check for
     * validity first and make sure the invocation is thread-safe.
     *
     * @return 
     */
    inline bool is_locked() const noexcept
    {
        return atomic.m.bits & bits_flag::lock;
    }
};
static_assert(std::is_standard_layout_v<dataslot_meta>);
static_assert(sizeof(dataslot_meta::atomic) == 8);
static_assert(sizeof(dataslot_meta) == 512_B);

/**
 * Slot in headless hashtable, packages user data and inline metadata.
 *
 * User data is packed ahead of metadata, for lock bit must be at the end of the
 * slot (see @ref dataslot_meta ).
 */
struct [[gnu::packed]] dataslot {
    using meta_type = dataslot_meta;
    using key_type = dataslot_meta::key_type;

    /**
     * Packed buffer, with handy helpers
     */
    struct [[gnu::packed]] value_type {
        uint8_t _d[DATA_SEG_LEN];

        /* constructors */
    public:
        value_type() noexcept {}
        /**
         * Initialize with given data
         * @param d source buffer
         * @param len length to be copied
         */
        value_type(const void *d, size_t len)
        {
            this->set(d, len);
        }

        /* helpers */
    public:
        /**
         * Assigns data
         *
         * @note
         * Unused part of the buffer should be zeroed-out, for data length is
         * not recorded here (or rather, due to our large-KV-spans-across-multiple-
         * consecutive-slots-design, valid segment size of the current slot is
         * not recorded at all), so we checksum on the entire block.
         *
         * @param d source data buffer
         * @param len length to be copied
         */
        inline void set(const void *d, size_t len)
        {
            if (len > sizeof(_d))
                throw std::invalid_argument("too large");
            memcpy(_d, d, len);
            memset(_d + len, 0, sizeof(_d) - len);
        }

        static inline uint32_t checksum(const void *d, size_t len) noexcept
        {
            return crc32_iscsi((uint8_t*)d, len, 0x1919810);
        }
        inline auto checksum() const noexcept
        {
            return checksum(_d, sizeof(_d));
        }
    } data;

    value_type data;
    meta_type meta;

    /* constructors */
public:
    /**
     * Default constructor, constructs invalid / unused slot.
     */
    dataslot() noexcept : meta() {}
    dataslot(const char *k, const void *d, size_t dlen)
    {
        /* optionally invalidate slot, setting data automatically causes checksum
            to mismatch */
        invalidate();
        data.set(d, dlen);
        meta.length = dlen;
        meta.data_crc = data.checksum();
        /* set valid flag at the end */
        meta.set_key(k);
    }

    /* required interface */
public:
    inline const key_type &key() const noexcept
    {
        return meta.key;
    }
    inline value_type &value() noexcept
    {
        return data;
    }

    inline void invalidate() noexcept
    {
        meta.invalidate();
    }
    inline bool is_valid() const noexcept
    {
        return (
                meta.key_validity() == 0
            && data.checksum() == meta.data_crc
        );
    }
    inline bool is_invalid() const noexcept
    {
        return !is_valid();
    }

    /* helpers */
public:
    /**
     * Check slot validity
     * @return
     * * 0 slot is valid, ready to be read
     * * -EINVAL if invalid or unused
     * * -ECOMM if checksum, either key or data, does not match
     * * -EAGAIN valid, but currently locked
     */
    inline int validity() const noexcept
    {
        if (auto kv = meta.key_validity(); kv)
            return kv;
        if (data.checksum() != meta.data_crc)
            return -ECOMM;
        if (meta.is_locked())
            return -EAGAIN;
        return 0;
    }
};
static_assert(std::is_standard_layout_v<dataslot>);
static_assert(sizeof(dataslot) % 512_B == 0);

}   /* namespace gestalt */
