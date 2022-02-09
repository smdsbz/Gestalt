/**
 * @file read.hpp
 *
 * Read operation
 */

#pragma once

#include "./base.hpp"


namespace gestalt {
namespace ops {

using namespace std;


class Read : Base {
public:
    using Base::buf;

private:
    ibv_sge sgl[1];
    ibv_send_wr wr[1];

    /* c/dtor */
public:
    Read(rdma_cm_id *_id, uint32_t _rkey) : Base(_id, _rkey)
    {
        sgl[0].addr = reinterpret_cast<uintptr_t>(buf.data());
        sgl[0].lkey = mr->lkey;

        wr[0].next = NULL;
        wr[0].sg_list = sgl; wr[0].num_sge = 1;
        wr[0].opcode = IBV_WR_RDMA_READ;
        wr[0].send_flags = IBV_SEND_SIGNALED;
        wr[0].wr.rdma.rkey = rkey;
    }

    /* interface */
public:
    int perform(void) const override
    {
        return Base::perform(wr);
    }

    inline void parameterize(uintptr_t addr, uint32_t length) noexcept
    {
        sgl[0].length = length;
        wr[0].wr.rdma.remote_addr = addr;
    }

};  /* class Read */

}   /* namespace ops */
}   /* namespace gestalt */
