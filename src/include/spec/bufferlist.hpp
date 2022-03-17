/**
 * @file bufferlist.hpp
 *
 * We have already defined the basic data structure gestalt::dataslot, however
 * it is effectively fixed-length, and gestalt::bufferlist is here to support
 * values larger than one dataslot.
 *
 * bufferlist is essentially a simple array of dataslots, with some indexing
 * helpers, nothing special. Such data layout allows for contiguous RDMA copying,
 * accelerating I/O on large data.
 */

#pragma once

/**
 * @def DEBUG_BUFFERLIST
 * turns on assert() reguardless of NDEBUG
 */
// #define DEBUG_BUFFERLIST

#include <array>
#include <stdexcept>
#include <cassert>
#include <cstdlib>

#include "./dataslot.hpp"
#include "../common/size_literals.hpp"
#include "./params.hpp"


namespace gestalt {

using namespace std;


/**
 * Array of dataslot, with helpers
 * @tparam NB size of the largest value you want this instance to hold, in bytes
 */
template<size_t NB>
struct bufferlist {
    constexpr static size_t nr_slots = ceil_div(NB, DATA_SEG_LEN);

    std::array<dataslot, nr_slots> arr;
    /**
     * @private
     * starting position (slot) of valid value, -1 for no valid data.
     */
    mutable ssize_t pos;
    /**
     * @private
     * (for read op only) amount of data (in slots) comming from remote,
     * -1 for no data yet
     */
    mutable ssize_t working_range = nr_slots;

    /* c/dtor */

    bufferlist() noexcept : pos(-1)
    { }

    /* interface */

    constexpr size_t max_size() const noexcept
    {
        return nr_slots * DATA_SEG_LEN;
    }
    /**
     * @return underlying array
     */
    inline dataslot *data() noexcept
    {
        return arr.data();
    }

    /**
     * Actual size of stored value, as reported by the first slot
     * @note check validity before use
     * @note The size reported by this method has nothing to do with how many
     * valid data the current object holds, the later should be tracked internally
     * while performing I/O operations. However, by the time I/O operator finishes
     * and returned to client, the two should be the same, or an exception would
     * have been thrown.
     * @return size of the value
     */
    inline size_t size() const
    {
        if (pos < 0)
            [[unlikely]] throw std::invalid_argument("pos");
        return arr[pos].size();
    }
    /**
     * @return number of dataslots the current valid data takes
     */
    inline size_t slots() const
    {
        return ceil_div(size(), DATA_SEG_LEN);
    }

    /**
     * Check bufferlist data validity
     * @note must set working_range before calling
     * @param key Key of object to find. We fetch data in bulk, there could be
     *      multiple valid slot, we need to know which to look for.
     * @return
     * * 0 - bufferlist all valid and ready to be read, #pos will be set appropriately
     * * -EAGAIN - is wanted data, but currently locked
     * * -ECOMM - is wanted key, but checksum mismatch, could be slot dirty or
     *      overwriting
     * * -EINVAL - if invalid, no initialized data of key
     * * -EOVERFLOW - wanted value larger than that of this bufferlist instance can hold
     * * -EREMOTE - part of data is still remote
     *      * size() will be positive if we read the first half of a large value,
     *          need to fetch again with a larger range.
     *      * size() will be zero if we read later half of a large value,
     *          possibly due to a change in cluster map and data has been migrated
     *          or just simply deleted and overwriten by some other client.
     *      * a corner case is size() will be positive but we did fetched enough
     *          data, but somewhere in between we fetched data of another key,
     *          that is when later half of data was overwriten by another client
     *          while fetching. However, this should have been prevented with our
     *          locking write design, for the CAS lock will fail on overwrite.
     */
    int validity(const dataslot::key_type &key) const noexcept
    {
        if (pos < 0 || working_range < 0)
            [[unlikely]] return -EINVAL;

        /* check the first block, i.e. header.
            if header does not match, start anew */
        do {
            const auto v = arr[pos].validity();
            if (!v)
                [[likely]] break;
            // assert(v);
            if (arr[pos].key() == key)
                [[likely]] return v;
            // assert(v && arr[pos].key != key);
            if (pos >= std::min(static_cast<size_t>(working_range), params::hht_search_length))
                [[unlikely]] return -EINVAL;
            pos++;
            return validity(key);
        } while (0);

        const size_t len = size();

        /* we read from the middle of a large value, the actual value size is
            somewhere ahead */
        if (len == 0)
            return -EREMOTE;

        /* shortcut: entire data is held in one slot
            note that validity of the first slot is already checked */
        if (len < DATA_SEG_LEN)
            [[likely]] return 0;

        /* the buffer can't hold entire value anyway */
        if (pos * DATA_SEG_LEN + len > max_size())
            [[unlikely]] return -EOVERFLOW;

        /* check the entire value */
        for (size_t i = 1, k = ceil_div(len, DATA_SEG_LEN); i < k; i++) {
            const auto &d = arr[pos + i];
            if (d.validity() || d.key() != key)
                [[unlikely]] return -EREMOTE;
        }
        return 0;
    }

    /* indexing helper */

    /**
     * Copy data [off, off + len) into out
     * @note validate before use
     * @param[out] out copy destination buffer
     * @param off starting point of copy
     * @param len length to copy
     */
    void take(void *out, size_t off, size_t len) const
    {
#ifdef DEBUG_BUFFERLIST
#ifndef NDEBUG
#define __DID_NOT_HAVE_NDEBUG
#define NDEBUG
#endif
#endif
        assert(!validity());

        if (len > size())
            [[unlikely]] throw std::overflow_error("len");

        /* only take something from the first slot */
        if (off == 0 && len <= DATA_SEG_LEN) {
            [[likely]] std::memcpy(out, arr[pos].value().get(), len);
            return;
        }

        size_t isrc = pos + off / DATA_SEG_LEN;
        off %= DATA_SEG_LEN;

        /* already checked with validity() */
        if (isrc * DATA_SEG_LEN + off + len > max_size())
            [[unlikely]] throw std::overflow_error("validity()");

        /* pre-align

            isrc-th slot
            v
            ________M ________M
              ^
              off  -- len -->
        */
        assert(off < DATA_SEG_LEN);
        if (off) {
            size_t run = DATA_SEG_LEN - off;
            std::memcpy(out, arr[isrc].value().get() + off, run);
            isrc++;
            out += run;
            len -= run;
        }
        /* should not use off from now */

        /* aligned middle part

            isrc-th slot
            v
            ________M ________M ________M
            [<- this part -->]     ^
            ------- len -----------'
        */
        while (len > DATA_SEG_LEN) {
            std::memcpy(out, arr[isrc].value().get(), DATA_SEG_LEN);
            isrc++;
            out += DATA_SEG_LEN;
            len -= DATA_SEG_LEN;
        }
        assert(len <= DATA_SEG_LEN);

        /* tail

            isrc-th slot
            v
            ________M
            -len-^
        */
        if (len) {
            std::memcpy(out, arr[isrc].value().get(), len);
        }
#ifdef __DID_NOT_HAVE_NDEBUG
#undef NDEBUG
#endif
    }


    /**
     * Copy data from din to this bufferlist, all checksum set
     * @note This resets everything of the bufferlist, #pos will be zeroed, and
     * range of #dlen will be overwriten reguardless of slot availability.
     * You may want to manage #pos and check availability of range overwriting
     * from a higher perspective before calling this.
     * @param key key name
     * @param din source data buffer
     * @param dlen length of data
     */
    void set(const dataslot::key_type &key, const void *din, size_t dlen)
    {
#ifdef DEBUG_BUFFERLIST
#ifndef NDEBUG
#define __DID_NOT_HAVE_NDEBUG
#define NDEBUG
#endif
#endif
        pos = 0;

        /* one slot holds it all */
        if (dlen <= DATA_SEG_LEN) {
            [[likely]] arr[0].reset(key, din, dlen);
            return;
        }

        if (dlen > max_size())
            [[unlikely]] throw std::overflow_error("len");

        size_t isrc = 0;
        size_t _dlen = dlen;    // saved #dlen

        /**
         * @note Do remember to zero length of all slots, and only set length on
         * the first slot. We relies on this to judge if we read from the middle
         * of a large value.
         * @see dataslot.hpp, validity(const char*)
         */

        /* aligned part

            isrc-th slot
            v
            ________ ______--
            ^^^^^^^^
            this part
        */
        while (dlen > DATA_SEG_LEN) {
            arr[isrc].reset(key, din, DATA_SEG_LEN);
            arr[isrc].meta.length = 0;
            isrc++;
            din = reinterpret_cast<uint8_t*>(din) + DATA_SEG_LEN;
            dlen -= DATA_SEG_LEN;
        }
        assert(dlen <= DATA_SEG_LEN);

        /* tail */
        if (dlen) {
            arr[isrc].reset(key, din, dlen);
            arr[isrc].meta.length = 0;
        }

        arr[0].meta.length = _dlen;
        assert(!validity(key));
#ifdef __DID_NOT_HAVE_NDEBUG
#undef NDEBUG
#endif
    }


    // void overwrite(void *in, size_t len, size_t off);

};  /* struct bufferlist*/

}   /* namespace gestalt */
