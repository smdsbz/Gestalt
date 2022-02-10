/**
 * @file base.hpp
 *
 * Common base class for all I/O operations
 */

#pragma once

#include <memory>
#include <utility>

#include <rdma/rdma_cma.h>
#include "common/boost_log_helper.hpp"

#include "spec/bufferlist.hpp"
#include "spec/params.hpp"


namespace gestalt {
namespace ops {

using namespace std;

/**
 * @note currently we don't bother support value larger than this
 */
constexpr size_t max_op_size = params::max_op_size;
constexpr unsigned max_poll = params::max_poll_retry;


class Base {
public:
    /**
     * stores read result or to-be-writen data
     */
    bufferlist<max_op_size> buf;

protected:
    /** RDMA fiber, or connection, whichever you prefer to call it */
    mutable rdma_cm_id *id;
    uint32_t rkey;

    struct __IbvMrDeleter {
        inline void operator()(ibv_mr *mr)
        {
            if (ibv_dereg_mr(mr))
                boost_log_errno_throw(ibv_dereg_mr);
        }
    };
    /** memory region containing #buf */
    unique_ptr<ibv_mr, __IbvMrDeleter> mr;

    /* c/dtor */
public:
    Base(rdma_cm_id *_id, uint32_t _rkey) : id(_id), rkey(_rkey)
    {
        /* get #buf ready for RDMA */
        ibv_mr *raw_mr = ibv_reg_mr(
            id->pd,
            (void*)&buf, sizeof(buf),
            IBV_ACCESS_LOCAL_WRITE);
        if (!raw_mr)
            boost_log_errno_throw(ibv_reg_mr);
        mr.reset(raw_mr);
    }

    /* interface */
protected:
    /**
     * common logic performing RDMA operation
     * @param[in] wr RDMA work request, preferably member of the derived class, so
     *      we don't have to construct every field everytime. The work request
     *      must generate one and only one work completion.
     * @param[out] bad_wr 
     * @param[out] wc 
     * @return 
     * * 0 ok
     * * -EBADR bad work request
     * * -ETIME waited too long on completion queue
     * * -ECOMM RDMA returned an error state
     */
    inline int perform(
        const ibv_send_wr *wr,
        ibv_send_wr* &bad_wr, ibv_wc &wc) const noexcept
    {
        if (ibv_post_send(id->qp, (ibv_send_wr*)wr, &bad_wr))
            [[unlikely]] return -EBADR;
        for (unsigned retry = max_poll; retry; --retry) {
            int ret = ibv_poll_cq(id->send_cq, 1, &wc);
            if (!ret)
                continue;
            if (ret < 0)
                [[unlikely]] return -ECOMM;
            return 0;
        }
        return -ETIME;
    }
    /**
     * default implementation of perform()
     * @sa perform(const ibv_send_wr*, ibv_send_wr*&, ibv_wc&)
     */
    virtual int perform(const ibv_send_wr *wr) const noexcept
    {
        ibv_send_wr *bad_wr;
        ibv_wc wc;
        return perform(wr, bad_wr, wc);
    }
public:
    /**
     * should be implemented as perform(ibv_send_wr&, ibv_send_wr*&, ibv_wc&)
     * while taking wr from derived
     * @return defined by derived
     */
    virtual int perform(void) const = 0;
    constexpr int operator()(void) const
    {
        return perform();
    }
    /**
     * Preferably, in the derived class, operator()(...) with non-empty parameter
     * list parameterizes the operation.
     */
};  /* class BaseOps */

}   /* namespace ops */
}   /* namespace gestalt */
