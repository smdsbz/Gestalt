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


namespace gestalt {

using namespace std;


/**
 * Array of dataslot, with helpers
 * @tparam NB size of the largest value you want this instance to hold, in bytes
 */
template<size_t NB>
class bufferlist {
    constexpr static size_t nr_slots = ceil_div(NB, DATA_SEG_LEN);
    std::array<dataslot, nr_slots> arr;

    /* c/dtor */
public:
    bufferlist() noexcept : arr()
    { }

    /* interface */
public:
    inline size_t max_size() noexcept
    {
        return nr_slots * DATA_SEG_LEN;
    }
    inline dataslot *data() noexcept
    {
        return arr.data();
    }

    /**
     * Actual size of stored value, as reported by the first slot
     * @note check validity() before use
     * @note The size reported by this method has nothing to do with how many
     * valid data the current object holds, the later should be tracked internally
     * while performing I/O operations. However, by the time I/O operator finishes
     * and returned to client, the two should be the same, or an exception would
     * have been thrown.
     * @return size of the value
     */
    inline size_t size() noexcept
    {
        return arr[0].size();
    }

    /**
     * Check bufferlist data validity
     * @return
     * * 0 bufferlist all valid, ready to be read
     * * -EINVAL if invalid, no initialized data
     * * -ECOMM checksum does not match
     * * -EAGAIN valid, but locked
     * * -EOVERFLOW wanted value larger than that of this instance can hold
     * * -EREMOTE part of data is still remote, i.e. within the reported data
     *      length encountered an invalid slot, you may fetch again
     */
    inline int validity() const noexcept
    {
        /* check the first block, i.e. header */
        if (int r = arr[0].validity(); r)
            return r;

        size_t len = size();

        /* entire data is held in one slot */
        if (len < DATA_SEG_LEN)
            [[likely]] return 0;

        if (len > max_size())
            return -EOVERFLOW;

        for (size_t i = 1, k = ceil_div(len, DATA_SEG_LEN); i < k; i++) {
            const auto &d = arr[i];
            if (d.validity())
                return -EREMOTE;
        }
        return 0;
    }

    /* indexing helper */
public:
    /**
     * Copy data [off, off + len) into out
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
        assert(validity() == 0);

        /* only take something from the first slot */
        if (off == 0 && len <= DATA_SEG_LEN) {
            [[likely]] std::memcpy(out, arr[0].value().get(), len);
            return;
        }

        if (off + len > max_size())
            throw std::overflow_error("off + len");

        size_t isrc = off / DATA_SEG_LEN;
        off %= DATA_SEG_LEN;

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
     * @param key key name
     * @param din source data buffer
     * @param dlen length of data
     */
    void set(const char *key, void *din, size_t dlen)
    {
#ifdef DEBUG_BUFFERLIST
#ifndef NDEBUG
#define __DID_NOT_HAVE_NDEBUG
#define NDEBUG
#endif
#endif
        /* one slot holds it all */
        if (dlen <= DATA_SEG_LEN) {
            [[likely]] arr[0].reset(key, din, dlen);
            return;
        }

        if (dlen > max_size())
            throw std::overflow_error("len");

        size_t isrc = 0;
        size_t _dlen = dlen;

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
            din += DATA_SEG_LEN;
            dlen -= DATA_SEG_LEN;
        }
        assert(dlen <= DATA_SEG_LEN);

        /* tail */
        if (dlen) {
            arr[isrc].reset(key, din, dlen);
            arr[isrc].meta.length = 0;
        }

        arr[0].meta.length = _dlen;

        assert(validity() == 0);
#ifdef __DID_NOT_HAVE_NDEBUG
#undef NDEBUG
#endif
    }


    // void overwrite(void *in, size_t len, size_t off);

};  /* class bufferlist*/

}   /* namespace gestalt */
