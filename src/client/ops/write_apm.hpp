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

#include "internal/ops_base.hpp"


namespace gestalt {
namespace ops {

using namespace std;


class WriteAPM final : public Base {
public:
    using Base::buf;
private:
    ibv_sge sgl[2];
    ibv_send_wr wr[2];

    /* c/dtor */
public:
    WriteAPM(ibv_pd *pd) : Base(pd)
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
    int perform(void) const override
    {
        return Base::perform(wr);
    }
    using Base::operator();

    /**
     * 
     * @note fill #buf before parameterizing
     * @param id 
     * @param addr justified remote VA of dataslot
     * @param rkey 
     */
    inline void parameterize(
        rdma_cm_id *id,
        uintptr_t addr, uint32_t rkey) noexcept
    {
        Base::id = id;
        sgl[0].length = buf.slots() * sizeof(dataslot);
        wr[0].wr.rdma.remote_addr = addr;
        wr[1].wr.rdma.remote_addr = addr;
    }
    inline WriteAPM &operator()(
        rdma_cm_id *id,
        uintptr_t addr, uint32_t rkey) noexcept
    {
        parameterize(id, addr, rkey);
        return *this;
    }


};  /* class WriteAPM */

}   /* namespace ops */
}   /* namespace gestalt */
