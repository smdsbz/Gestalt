/**
 * @file ops_base.hpp
 *
 * Common base class for all I/O operations
 */

#pragma once

#include <memory>
#include <utility>

#include <rdma/rdma_cma.h>
#include "../common/boost_log_helper.hpp"

#include "../spec/bufferlist.hpp"
#include "../spec/params.hpp"
#include "optim.hpp"


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
    mutable bufferlist<max_op_size> buf;
private:
    virtual string opname() const noexcept = 0;

protected:
    struct __IbvMrDeleter {
        inline void operator()(ibv_mr *mr)
        {
            if (ibv_dereg_mr(mr))
                boost_log_errno_throw(ibv_dereg_mr);
        }
    };
    /** memory region containing #buf */
    unique_ptr<ibv_mr, __IbvMrDeleter> mr;

    /* common parameters */

    rdma_cm_id *id;
    /**
     * shared completion queue
     * @sa gestalt::optimization::batched_poll
     */
    ibv_cq *scq;

    /* c/dtor */
public:
    /**
     * @param pd 
     * @param scq shared completion queue, if used
     * @sa gestalt::optimization::batched_poll
     */
    Base(ibv_pd *pd, ibv_cq *scq_) : scq(scq_)
    {
        /* get #buf ready for RDMA */
        ibv_mr *raw_mr = ibv_reg_mr(
            pd,
            (void*)&buf, sizeof(buf),
            IBV_ACCESS_LOCAL_WRITE);
        if (!raw_mr)
            boost_log_errno_throw(ibv_reg_mr);
        mr.reset(raw_mr);

        /* sanity check for scq */
        if (optimization::batched_poll && !scq) {
            errno = -EINVAL;
            boost_log_errno_throw(ops::Base);
        }
    }

    /* interface */
protected:
    /**
     * common logic performing RDMA operation
     *
     * the method is virutal so you may repurpose it entirely
     *
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
     * * -ECANCELED RDMA returned error in completion
     * * -EOVERFLOW polled work completion more than requested
     */
    virtual int perform(
        const ibv_send_wr *wr,
        ibv_send_wr* &bad_wr, ibv_wc &wc) const noexcept
    {
        if (ibv_post_send(id->qp, const_cast<ibv_send_wr*>(wr), &bad_wr))
            [[unlikely]] return -EBADR;
        for (unsigned retry = max_poll; true || retry; --retry) {
            int r;
            [[likely]] if constexpr (optimization::batched_poll) {
                r = ibv_poll_cq(scq, 1, &wc);
            }
            else {
                r = ibv_poll_cq(id->send_cq, 1, &wc);
            }
            if (r == 1)
                [[likely]] return 0;
            if (!r)
                [[likely]] continue;
            if (r < 0)
                return -ECOMM;
            if (wc.status != IBV_WC_SUCCESS)
                return -ECANCELED;
            return -EOVERFLOW;
        }
        return -ETIME;
    }
    /**
     * default implementation of perform()
     * @sa perform(const ibv_send_wr*, ibv_send_wr*&, ibv_wc&) const
     */
    inline int perform(const ibv_send_wr *wr) const noexcept
    {
        ibv_send_wr *bad_wr;
        ibv_wc wc;
        int r = perform(wr, bad_wr, wc);
        if (r == -ECANCELED) {
            [[unlikely]] BOOST_LOG_TRIVIAL(error)
                << opname()
                << " polled unhealthy work completion: "
                << ibv_wc_status_str(wc.status);
        }
        return r;
    }
public:
    /**
     * should be implemented as wrapper around
     * perform(const ibv_send_wr *wr, ibv_send_wr*&, ibv_wc&) while taking
     * #id and #wr from derived
     * @return 
     */
    virtual int perform(void) const = 0;
    inline int operator()(void) const
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
