/**
 * @file read.hpp
 *
 * Read operation
 */

#pragma once

#include "internal/ops_base.hpp"


namespace gestalt {
namespace ops {

using namespace std;


class Read : public Base {
public:
    using Base::buf;
private:
    ibv_sge sgl[1];
    ibv_send_wr wr[1];

    /* c/dtor */
public:
    Read(ibv_pd *pd) : Base(pd)
    {
        sgl[0].addr = reinterpret_cast<uintptr_t>(buf.data());
        sgl[0].lkey = mr->lkey;

        wr[0].next = NULL;
        wr[0].sg_list = sgl; wr[0].num_sge = 1;
        wr[0].opcode = IBV_WR_RDMA_READ;
        wr[0].send_flags = IBV_SEND_SIGNALED;
    }

    /* interface */
public:
    int perform(void) const override
    {
        return Base::perform(wr);
    }
    using Base::operator();

    inline void parameterize(
        rdma_cm_id *id,
        uintptr_t addr, uint32_t length, uint32_t rkey) noexcept
    {
        Base::id = id;
        sgl[0].length = length;
        wr[0].wr.rdma.remote_addr = addr;
    }
    inline Read &operator()(
        rdma_cm_id *id,
        uintptr_t addr, uint32_t length, uint32_t rkey) noexcept
    {
        parameterize(id, addr, length, rkey);
        return *this;
    }

};  /* class Read */

}   /* namespace ops */
}   /* namespace gestalt */
