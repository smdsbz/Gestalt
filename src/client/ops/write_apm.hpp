/**
 * @file write_apm.hpp
 *
 * Write operation, with Application Persistency (APM), i.e. RDMA Write followed
 * by a random RDMA Read flushing write that may be still residing in RNIC.
 *
 * This implementation is only an RDMA op, it simply overwrites a remote region
 * and makes sure it is persistent on return. No gestalt::dataslot availability
 * check is performed, writing data while expanding or shrinking value size
 * should be handled by other higher order code.
 */

#pragma once

#include <vector>

#include "internal/ops_base.hpp"
#include "optim.hpp"


namespace gestalt {
namespace ops {

using namespace std;


/**
 * Parallel write
 */
class WriteAPM final : public Base {
public:
    using Base::buf;
    struct target_t {
        rdma_cm_id *id;
        uintptr_t addr;
        uint32_t rkey;
    public:
        target_t(rdma_cm_id *_id, uintptr_t _addr, uint32_t _rkey) noexcept :
            id(_id), addr(_addr), rkey(_rkey)
        { }
    };

    /**
     * ranks of successfully writes
     */
    mutable vector<unsigned> success_polls;

private:
    ibv_sge sgl[2];
    mutable ibv_send_wr wr[2];
    /**
     * If writing to a primary set, the first replica, aka the primary replica,
     * should be left in locked state. A separate Unlock op will unlock it in
     * the end.
     */
    bool is_primary_set;
    vector<target_t> targets;

    string opname() const noexcept override
    {
        return "WriteAPM";
    }

    /* c/dtor */
public:
    WriteAPM(ibv_pd *pd, ibv_cq *scq) : Base(pd, scq)
    {
        /* Write */
        sgl[0].addr = reinterpret_cast<uintptr_t>(buf.data());
        sgl[0].lkey = mr->lkey;

        wr[0].next = &wr[1];
        wr[0].sg_list = &sgl[0]; wr[0].num_sge = 1;
        wr[0].opcode = IBV_WR_RDMA_WRITE;
        wr[0].send_flags = 0;

        /* Flush */
        sgl[1].addr = reinterpret_cast<uintptr_t>(buf.data());
        sgl[1].length = 1;
        sgl[1].lkey = mr->lkey;

        wr[1].next = NULL;
        wr[1].sg_list = &sgl[1]; wr[0].num_sge = 1;
        wr[1].opcode = IBV_WR_RDMA_READ;
        wr[1].send_flags = IBV_SEND_SIGNALED;
    }

    /* interface */
public:
    /**
     * 
     * @note fill #buf before parameterizing
     * @param vec 
     * @param primary if writing to primary set
     */
    inline void parameterize(const vector<target_t> &vec, bool primary) noexcept
    {
        targets = vec;
        is_primary_set = primary;
        sgl[0].length = buf.slots() * sizeof(dataslot);
    }
    inline WriteAPM &operator()(const vector<target_t> &vec, bool primary) noexcept
    {
        parameterize(vec, primary);
        return *this;
    }

    /**
     * 
     * @param wr 
     * @param bad_wr 
     * @param wc 
     * @return 
     * * 0 ok
     * * -EBADR bad work request
     * * ...
     */
    int perform(
        const ibv_send_wr *wr,
        ibv_send_wr* &bad_wr, ibv_wc &wc) const noexcept override
    {
        assert(wr == this->wr);

        /* emit requests */

        if (is_primary_set) {
            auto &header_flag = buf.arr[0].meta.atomic.m.bits;
            header_flag |= dataslot::meta_type::bits_flag::lock;

            const auto &prim = targets.at(0);
            this->wr[0].wr.rdma.remote_addr = prim.addr;
            this->wr[0].wr.rdma.rkey = prim.rkey;
            this->wr[1].wr_id = 0;
            this->wr[1].wr.rdma.remote_addr = prim.addr;
            this->wr[1].wr.rdma.rkey = prim.rkey;
            if (ibv_post_send(prim.id->qp, this->wr, &bad_wr))
                [[unlikely]] return -EBADR;

            /**
             * HACK: lock bit on secondaries doesn't actually do anything, leave
             * it locked.
             */
            // header_flag &= ~dataslot::meta_type::bits_flag::lock;
        }

        for (unsigned r = is_primary_set ? 1 : 0; r < targets.size(); r++) {
            const auto &t = targets.at(r);
            this->wr[0].wr.rdma.remote_addr = t.addr;
            this->wr[0].wr.rdma.rkey = t.rkey;
            this->wr[1].wr_id = r;
            this->wr[1].wr.rdma.remote_addr = t.addr;
            this->wr[1].wr.rdma.rkey = t.rkey;
            if (ibv_post_send(t.id->qp, this->wr, &bad_wr))
                [[unlikely]] return -EBADR;
        }

        /* poll from all channels */

        if constexpr (optimization::batched_poll) {
            /* fake success to deligate */
            wc.status = IBV_WC_SUCCESS;

            success_polls.clear();
            success_polls.reserve(targets.size());

            ibv_wc wcbuf[8];
            unsigned remain = targets.size();
            for (unsigned retry = max_poll; remain && (true || retry); --retry) {
                int c;
                [[likely]] c = ibv_poll_cq(scq, remain, wcbuf);
                remain -= c;
                if (!c)
                    [[unlikely]] continue;
                if (c < 0)
                    [[unlikely]] return -ECOMM;
                for (unsigned cc = 0; cc < c; cc++) {
                    if (wcbuf[cc].status != IBV_WC_SUCCESS) {
                        [[unlikely]] std::memcpy(&wc, &wcbuf[cc], sizeof(wc));
                        return -ECANCELED;
                    }
                    success_polls.push_back(wcbuf[cc].wr_id);
                }
            }
            if (remain)
                [[unlikely]] return -ETIME;
        }
        else {
            for (const auto &t : targets) {
                int r;
                for (unsigned retry = max_poll; true || retry; --retry) {
                    [[likely]] r = ibv_poll_cq(t.id->send_cq, 1, &wc);
                    if (!r)
                        [[unlikely]] continue;
                    if (r < 0)
                        [[unlikely]] return -ECOMM;
                    if (wc.status != IBV_WC_SUCCESS)
                        [[unlikely]] return -ECANCELED;
                    break;
                }
                if (!r)
                    [[unlikely]] return -ETIME;
            }
        }

        return 0;
    }
    int perform(void) const override
    {
        return Base::perform(wr);
    }
    using Base::operator();

};  /* class WriteAPM */

}   /* namespace ops */
}   /* namespace gestalt */
