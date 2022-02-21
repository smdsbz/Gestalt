/**
 * @file lock.hpp
 *
 * Lock operation
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

    using flag_t = dataslot::meta_type::bits_flag;
    using atomic_t = decltype(dataslot::meta_type::atomic);

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
     * @param addr justified remote VA of the dataslot, offset to atomic field
     *      will be calculated internally
     * @param khx key hash (crc32_iscsi(), see dataslot.hpp)
     * @param rkey 
     */
    inline void parameterize(
        rdma_cm_id *id,
        uintptr_t addr, uint32_t khx, uint32_t rkey) noexcept
    {
        Base::id = id;
        wr[0].wr.atomic.remote_addr = addr + offsetof(dataslot, meta.atomic);
        {
            atomic_t a(khx);
            /* before is unlocked */
            a.m.bits = flag_t::valid;
            wr[0].wr.atomic.compare_add = a.u64;
            /* after is locked */
            a.m.bits = flag_t::valid | flag_t::lock;
            wr[0].wr.atomic.swap = a.u64;
        }
        wr[0].wr.atomic.rkey = rkey;
    }
    inline Lock &operator()(
        rdma_cm_id *id,
        uintptr_t addr, uint32_t khx, uint32_t rkey) noexcept
    {
        parameterize(id, addr, khx, rkey);
        return *this;
    }
    inline Lock &operator()(
        rdma_cm_id *id,
        uintptr_t addr, const dataslot::key_type &key, uint32_t rkey) noexcept
    {
        parameterize(id, addr, key.hash(), rkey);
        return *this;
    }

    /**
     * 
     * @return 
     * * 0 successfully locked slot
     * * -EINVAL slot not initialized, i.e. slot is available
     * * -EBUSY slot write-locked
     * * -EBADF key fingerprint mismatch
     * * other see ops::Base::perform(const ibv_send_wr*)
     */
    int perform(void) const override
    {
        if (int r = Base::perform(wr); r)
            return r;

        const auto &before = *reinterpret_cast<const atomic_t*>(&wr[0].wr.atomic.compare_add);  // expected
        const auto &old = *reinterpret_cast<atomic_t*>(sgl[0].addr);    // remote old
        if (old.u64 == before.u64)
            [[likely]] return 0;

        BOOST_LOG_TRIVIAL(trace) << std::hex
            << "ops::Lock before " << before.u64
            << " old " << old.u64
            << std::dec;

        if (!(old.m.bits & flag_t::valid))
            [[unlikely]] return -EINVAL;
        if (old.m.bits & flag_t::lock)
            [[likely]] return -EBUSY;
        if (old.m.key_crc != before.m.key_crc)
            return -EBADF;

        throw std::runtime_error("unreachable");
    }
    using Base::operator();

};  /* class Lock */


class Unlock : public Base {
public:
    using Base::buf;
private:
    ibv_sge sgl[1];
    ibv_send_wr wr[1];

    using flag_t = dataslot::meta_type::bits_flag;
    using atomic_t = decltype(dataslot::meta_type::atomic);

    /* c/dtor */
public:
    Unlock(ibv_pd *pd) : Base(pd)
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
     * @param addr justified remote VA of the dataslot, offset to atomic field
     *      will be calculated internally
     * @param khx key hash (crc32_iscsi(), see dataslot.hpp)
     * @param rkey 
     */
    inline void parameterize(
        rdma_cm_id *id,
        uintptr_t addr, uint32_t khx, uint32_t rkey) noexcept
    {
        Base::id = id;
        wr[0].wr.atomic.remote_addr = addr + offsetof(dataslot, meta.atomic);
        {
            atomic_t a(khx);
            /* before is locked */
            a.m.bits = flag_t::valid | flag_t::lock;
            wr[0].wr.atomic.compare_add = a.u64;
            /* after is unlocked */
            a.m.bits = flag_t::valid;
            wr[0].wr.atomic.swap = a.u64;
        }
        wr[0].wr.atomic.rkey = rkey;
    }
    inline Unlock &operator()(
        rdma_cm_id *id,
        uintptr_t addr, uint32_t khx, uint32_t rkey) noexcept
    {
        parameterize(id, addr, khx, rkey);
        return *this;
    }
    inline Unlock &operator()(
        rdma_cm_id *id,
        uintptr_t addr, const dataslot::key_type &key, uint32_t rkey) noexcept
    {
        parameterize(id, addr, key.hash(), rkey);
        return *this;
    }

    /**
     * 
     * @return 
     * * 0 successfully unlocked slot
     * * -ECANCELED unlock failed
     * * other see ops::Base::perform(const ibv_send_wr*)
     */
    int perform(void) const override
    {
        if (int r = Base::perform(wr); r)
            return r;

        const auto &before = *reinterpret_cast<const atomic_t*>(&wr[0].wr.atomic.compare_add);  // expected
        const auto &old = *reinterpret_cast<atomic_t*>(sgl[0].addr);    // remote old
        if (old.u64 == before.u64)
            [[likely]] return 0;
        return -ECANCELED;
    }
    using Base::operator();

};  /* class Unlock */

}   /* namespace ops */
}   /* namespace gestalt */
