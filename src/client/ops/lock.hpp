/**
 * @file lock.hpp
 *
 * Un/Lock operation
 */

#pragma once

#include "internal/ops_base.hpp"


namespace gestalt {
namespace ops {

using namespace std;


class Lock : public Base {
public:
    using Base::buf;
private:
    ibv_sge sgl[1];
    ibv_send_wr wr[1];

    /* c/dtor */
public:
    Lock(ibv_pd *pd) : Base(pd)
    {
        sgl[0].addr = reinterpret_cast<uintptr_t>(buf.data());
        sgl[0].length = 8;  // which ever value is okay, as atomic is always 64b
        sgl[0].lkey = mr->lkey;

        wr[0].next = NULL;
        wr[0].sg_list = sgl; wr[0].num_sge = 1;
        wr[0].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wr[0].send_flags = IBV_SEND_SIGNALED;
    }

    /* interface */
public:
    /**
     * @param id 
     * @param addr remote VA of the dataslot, offset to atomic field will be
     *      calculated internally
     * @param khx key hash (crc32_iscsi(), see dataslot.hpp)
     * @param is_lock is parameterizing lock or unlock
     * @param rkey 
     */
    inline void parameterize(
        rdma_cm_id *id,
        uintptr_t addr, uint32_t khx, bool is_lock,
        uint32_t rkey) noexcept
    {
        Base::id = id;
        wr[0].wr.atomic.remote_addr = addr + offsetof(dataslot, meta.atomic);
        {
            /* unlocked state value */
            uint64_t vu =
                static_cast<uint64_t>(khx) << 32 |
                dataslot::meta_type::bits_flag::valid;
            /* locked state value */
            uint64_t vl = vu | dataslot::meta_type::bits_flag::lock;
            if (is_lock) {
                wr[0].wr.atomic.compare_add = vu;
                wr[0].wr.atomic.swap = vl;
            }
            else {
                wr[0].wr.atomic.compare_add = vl;
                wr[0].wr.atomic.swap = vu;
            }
        }
        wr[0].wr.atomic.rkey = rkey;
    }
    inline Lock &operator()(
        rdma_cm_id *id,
        uintptr_t addr, uint32_t khx, bool is_lock,
        uint32_t rkey) noexcept
    {
        parameterize(id, addr, khx, is_lock, rkey);
        return *this;
    }
    inline Lock &operator()(
        rdma_cm_id *id,
        uintptr_t addr, const dataslot::key_type &key, bool is_lock,
        uint32_t rkey) noexcept
    {
        parameterize(id, addr, key.hash(), is_lock, rkey);
        return *this;
    }

    /**
     * @see gestalt::Base::perform(void)
     * @return 
     * * 0 CAS successful
     * * -ECANCELED compare failed and no swap performed
     * * other
     */
    int perform(void) const override
    {
        if (int r = Base::perform(wr); r)
            return r;
        if (*reinterpret_cast<uint64_t*>(sgl[0].addr) != wr[0].wr.atomic.compare_add)
            return -ECANCELED;
        return 0;
    }
    using Base::operator();

};  /* class Lock */

}   /* namespace ops */
}   /* namespace gestalt */
