#define BOOST_TEST_MODULE rdma hello world
#include <boost/test/unit_test.hpp>

#include <iostream>
using std::cout, std::endl, std::flush, std::string;
#include <functional>
template<typename T> using deleted_unique_ptr = std::unique_ptr<T, std::function<void(T*)>>;
#define CONCAT(a, b) a ## b
#define __defer_impl(ptr, dfn, l)                               \
    std::unique_ptr<std::remove_pointer<decltype(ptr)>::type,   \
                    std::function<void(decltype(ptr))>>         \
    CONCAT(__dtor, l) (ptr, [&](decltype(ptr) p) { dfn(p); })
#define defer(ptr, dfn) __defer_impl(ptr, dfn, __LINE__)
#include <cerrno>
#include <future>
using std::packaged_task, std::future;
#include <thread>
using std::thread;
#include <chrono>
using namespace std::chrono_literals;

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

#include "playground/playground.h"

#define DVAR(V) do {                        \
    std::cout << #V "=" << V << std::endl;  \
} while(0)


BOOST_AUTO_TEST_CASE(boost_utf_hello_world) {
    BOOST_TEST(true);
}

BOOST_AUTO_TEST_CASE(ibv_hello_world) {
    /* open device */
    deleted_unique_ptr<ibv_device *> devices(
        ibv_get_device_list(NULL),
        [](auto p) { ibv_free_device_list(p); }
    );
    BOOST_TEST_REQUIRE(devices.get(), "no RDMA devices listed, please check your environment");
    for (size_t i = 0; devices.get()[i] != NULL; i++) {
        const auto &dev = devices.get()[i];
        DVAR(dev->name); DVAR(dev->dev_name); DVAR(dev->dev_path);
        DVAR(dev->ibdev_path); cout << endl;
    }
    auto device = devices.get()[0];
    BOOST_TEST(std::strcmp(device->name, ibv_get_device_name(device)) == 0);
    deleted_unique_ptr<ibv_context> rdma_ctx(
        ibv_open_device(device),
        [](auto p) { ibv_close_device(p); }
    );
    BOOST_REQUIRE(rdma_ctx.get());
    DVAR(rdma_ctx->num_comp_vectors);
    BOOST_REQUIRE_GE(rdma_ctx->num_comp_vectors, 1);
    ibv_device_attr dev_attr;
    BOOST_CHECK_EQUAL(ibv_query_device(rdma_ctx.get(), &dev_attr), 0);
    DVAR(dev_attr.max_qp); DVAR(dev_attr.max_qp_wr); DVAR(dev_attr.max_sge);
    cout << endl;

    /* prepare device */
    deleted_unique_ptr<ibv_pd> pd(
        ibv_alloc_pd(rdma_ctx.get()),
        [](auto p) { ibv_dealloc_pd(p); }
    );
    BOOST_REQUIRE(pd.get());
    deleted_unique_ptr<ibv_cq> cq(
        ibv_create_cq(rdma_ctx.get(), 100, NULL, NULL, 0),
        [](auto p) { ibv_destroy_cq(p); }
    );
    BOOST_REQUIRE(cq.get());
    /* test max_inline_data */
    uint32_t MAX_INLINE_DATA = 512;
    ibv_qp_init_attr qp_attr{
        .send_cq = cq.get(), .recv_cq = cq.get(),
        .cap = {.max_send_wr = 128, .max_recv_wr = 128,
                .max_send_sge = 32, .max_recv_sge = 32,
                .max_inline_data = MAX_INLINE_DATA/*to be tested*/},
        .qp_type = IBV_QPT_RC
    };
    for (;; MAX_INLINE_DATA <<= 1) {
        qp_attr.cap.max_inline_data = MAX_INLINE_DATA;
        deleted_unique_ptr<ibv_qp> test_qp(
            ibv_create_qp(pd.get(), &qp_attr),
            [](auto p) { ibv_destroy_qp(p); }
        );
        BOOST_TEST_WARN(test_qp.get(), "max_inline_data failed at " << MAX_INLINE_DATA);
        if (test_qp.get() == NULL) {
            MAX_INLINE_DATA >>= 1;
            qp_attr.cap.max_inline_data = MAX_INLINE_DATA;
            DVAR(MAX_INLINE_DATA);
            break;
        }
    }
    deleted_unique_ptr<ibv_qp> qp(
        ibv_create_qp(pd.get(), &qp_attr),
        [](auto p) { ibv_destroy_qp(p); }
    );
    if (!qp.get()) {
        if (errno == EINVAL)
            BOOST_TEST_REQUIRE(qp.get(), "ibv_create_qp() sometimes just fails "
                                "with 22 (invalid argument), it just does this");
        BOOST_TEST_REQUIRE(qp.get(), std::strerror(errno));
    }

    /* prepare RDMA memory */
    constexpr auto MEM_SIZE = 1024u;
    auto remote_memory = std::make_unique<uint8_t[]>(MEM_SIZE);
    deleted_unique_ptr<ibv_mr> remote_mr(
        ibv_reg_mr(pd.get(), remote_memory.get(), MEM_SIZE,
            IBV_ACCESS_LOCAL_WRITE |
            IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
            IBV_ACCESS_REMOTE_ATOMIC),
        [](auto p) { ibv_dereg_mr(p); }
    );
    BOOST_TEST_REQUIRE(remote_mr.get(), std::strerror(errno));
    DVAR(remote_mr->lkey); DVAR(remote_mr->rkey);
    auto local_memory = std::make_unique<uint8_t[]>(MEM_SIZE);
    deleted_unique_ptr<ibv_mr> local_mr(
        ibv_reg_mr(pd.get(), local_memory.get(), MEM_SIZE, IBV_ACCESS_LOCAL_WRITE),
        [](auto p) { ibv_dereg_mr(p); }
    );
    BOOST_REQUIRE(local_mr.get());
    DVAR(local_mr->lkey); DVAR(local_mr->rkey);
    cout << endl;

    /**
     * NOTE: use RDMACM for connection, not having to manually `ibv_modify_qp()`
     * makes your life a lot easier
     */
}

/**
 * Steps:
 * 1. Establish connection
 * 2. Server SEND
 * 3. Client READ
 * 4. Client WRITE
 */
BOOST_AUTO_TEST_CASE(rdma_hello_world_threaded) {
    cout << "==== Entering rdma_hello_world_threaded ====" << endl;
    constexpr size_t MEM_SIZE = 512;
    auto server_mem = std::make_unique<uint8_t[]>(MEM_SIZE);
    std::memcpy(server_mem.get(), "1145141919810", sizeof("1145141919810")-1);
    ibv_mr *server_msg_mr;

    bool is_client_done = false;

    thread __server_thread([&] {
        /* create server stub */
        rdma_addrinfo *addrinfo;
        rdma_addrinfo server_addr_hint{
            .ai_flags = RAI_PASSIVE, .ai_port_space = RDMA_PS_TCP,
        };
        BOOST_REQUIRE(!rdma_getaddrinfo(PLAYGROUND_RNIC_IP, PLAYGROUND_RNIC_PORT, &server_addr_hint, &addrinfo));
        defer(addrinfo, rdma_freeaddrinfo);
        rdma_cm_id *server_id;
        ibv_qp_init_attr init_attr{
            .cap = { .max_send_wr = 16, .max_recv_wr = 16,
                     .max_send_sge = 16, .max_recv_sge = 16,
                     .max_inline_data = 512 },
            .qp_type = IBV_QPT_RC,
            .sq_sig_all = 1
        };
        BOOST_REQUIRE(!rdma_create_ep(&server_id, addrinfo, NULL, &init_attr));
        defer(server_id, rdma_destroy_ep);
        DVAR(server_id->verbs);
        DVAR(server_id->send_cq); DVAR(server_id->recv_cq); DVAR(server_id->srq);
        DVAR(server_id->qp); DVAR(server_id->qp_type);
        // DVAR(server_id->qp->send_cq); DVAR(server_id->qp->recv_cq);
        DVAR(server_id->pd);
        // DVAR(server_id->pd->handle);
        cout << endl;

        /* start server */
        BOOST_REQUIRE(!rdma_listen(server_id, 0));
        /* listen for incoming connections */
        rdma_cm_id *connected_client_id;
        BOOST_REQUIRE(!rdma_get_request(server_id, &connected_client_id));
        defer(connected_client_id, rdma_destroy_ep);
        /* accept incoming connection */
        BOOST_REQUIRE(!rdma_accept(connected_client_id, NULL));
        defer(connected_client_id, rdma_disconnect);
        cout << "Server (agent) state after connect" << endl;
        DVAR(connected_client_id->verbs);
        DVAR(connected_client_id->send_cq); DVAR(connected_client_id->recv_cq); DVAR(connected_client_id->srq);
        DVAR(connected_client_id->qp); DVAR(connected_client_id->qp_type);
        DVAR(connected_client_id->qp->send_cq); DVAR(connected_client_id->qp->recv_cq);
        DVAR(connected_client_id->pd); DVAR(connected_client_id->pd->handle);
        cout << endl;
        BOOST_CHECK_EQUAL(connected_client_id->send_cq, connected_client_id->qp->send_cq);
        BOOST_CHECK_EQUAL(connected_client_id->recv_cq, connected_client_id->qp->recv_cq);
        /* prepare RDMA-enabled memory */
        BOOST_REQUIRE((server_msg_mr = ibv_reg_mr(
            connected_client_id->pd, server_mem.get(), MEM_SIZE,
            IBV_ACCESS_LOCAL_WRITE |
            IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE |
            IBV_ACCESS_REMOTE_ATOMIC
        )));
        defer(server_msg_mr, ibv_dereg_mr);

        /* 2. RDMA SEND */
        {
            cout << "Server RDMA SEND" << endl;
            ibv_sge sgl[] = {
                {.addr = (uintptr_t)server_mem.get(), .length = 8, .lkey = server_msg_mr->lkey},
            };
            ibv_send_wr swr{
                .next = NULL,
                .sg_list = sgl, .num_sge = 1,
                .opcode = IBV_WR_SEND,
            }, *bad_swr;
            BOOST_REQUIRE(!ibv_post_send(connected_client_id->qp, &swr, &bad_swr));
            ibv_wc wc;
            ssize_t poll_spins = 0;
            while (!ibv_poll_cq(connected_client_id->send_cq, 1, &wc)) poll_spins++;
            cout << "server poll spinned for " << poll_spins << " times before non-empty" << endl;
            BOOST_CHECK_EQUAL(wc.status, IBV_WC_SUCCESS);
            BOOST_CHECK_EQUAL(wc.opcode, IBV_WC_SEND);
            cout << "server polled send " << wc.opcode << endl;
            cout << endl;
        }

        /** 3. handle READ
         * 4. handle WRITE
         *
         * I.e. do nothing, since it's RDMA-ed.
         *
         * However, we still need RDMA-enabled memory (and PD and QP etc) to be
         * available, otherwise client will fail to read anything (and get blocked
         * forever).
         */

        while (!is_client_done) std::this_thread::sleep_for(.1s);
    });

    thread __client_thread([&] {
        /* create client stub */
        rdma_addrinfo *addrinfo;
        rdma_addrinfo addr_hint{
            .ai_port_space = RDMA_PS_TCP
        };
        BOOST_REQUIRE(!rdma_getaddrinfo(PLAYGROUND_RNIC_IP, PLAYGROUND_RNIC_PORT, &addr_hint, &addrinfo));
        defer(addrinfo, rdma_freeaddrinfo);
        rdma_cm_id *client_id;
        ibv_qp_init_attr init_attr{
            .cap = { .max_send_wr = 16, .max_recv_wr = 16,
                     .max_send_sge = 16, .max_recv_sge = 16,
                     .max_inline_data = 512 },
            .qp_type = IBV_QPT_RC,
            .sq_sig_all = 1
        };
        BOOST_REQUIRE(!rdma_create_ep(&client_id, addrinfo, NULL, &init_attr));
        defer(client_id, rdma_destroy_ep);
        cout << "Client state after end-point creation" << endl;
        DVAR(client_id->verbs);
        DVAR(client_id->send_cq);  DVAR(client_id->recv_cq); DVAR(client_id->srq);
        DVAR(client_id->qp); DVAR(client_id->qp_type);
        DVAR(client_id->qp->send_cq); DVAR(client_id->qp->recv_cq);
        DVAR(client_id->pd); DVAR(client_id->pd->handle);
        cout << endl;
        BOOST_CHECK_EQUAL(client_id->send_cq, client_id->qp->send_cq);
        BOOST_CHECK_EQUAL(client_id->recv_cq, client_id->qp->recv_cq);
        /* prepare RDMA-enabled memory */
        auto msg_mem = std::make_unique<uint8_t[]>(MEM_SIZE);
        ibv_mr *msg_mr;
        BOOST_REQUIRE((msg_mr = ibv_reg_mr(
            client_id->pd, msg_mem.get(), MEM_SIZE,
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE
        )));
        defer(msg_mr, ibv_dereg_mr);

        /* client try connect to server, server accept connection in other thread */
        BOOST_TEST_REQUIRE(!rdma_connect(client_id, NULL), std::strerror(errno));
        cout << "client connected" << endl;
        defer(client_id, [](rdma_cm_id *p) {
            BOOST_TEST_CHECK(!rdma_disconnect(p), std::strerror(errno));
            cout << "client disconnected" << endl;
        });
        BOOST_REQUIRE_EQUAL(client_id->qp->state, IBV_QPS_RTS);
        ibv_qp_attr queried_qp_attr;
        ibv_qp_init_attr queried_init_attr;
        BOOST_REQUIRE(!ibv_query_qp(client_id->qp, &queried_qp_attr, 0, &queried_init_attr));

        /* 2. recv */
        {
            ibv_sge sgl[] = {
                {.addr = (uintptr_t)msg_mem.get(), .length = 8, .lkey = msg_mr->lkey},
            };
            ibv_recv_wr wr{
                .next = NULL,
                .sg_list = sgl, .num_sge = 1,
            };
            ibv_recv_wr *bad_rwr;
            BOOST_REQUIRE(!ibv_post_recv(client_id->qp, &wr, &bad_rwr));
            ibv_wc wc;
            ssize_t poll_spins = 0;
            while (!ibv_poll_cq(client_id->recv_cq, 1, &wc)) poll_spins++;
            cout << "client poll spinned for " << poll_spins << " times before non-empty" << endl;
            BOOST_CHECK_EQUAL(wc.status, IBV_WC_SUCCESS);
            BOOST_CHECK_EQUAL(wc.opcode, IBV_WC_RECV);
            cout << "client polled recv " << wc.opcode << "\n"
                << "byte_len " << wc.byte_len << endl;
            std::printf("server_mem.get(): %s\nmsg_mem.get(): %s\n",
                    server_mem.get(), msg_mem.get());
            BOOST_CHECK_EQUAL(string("11451419"), string(reinterpret_cast<char*>(msg_mem.get())));
            cout << endl;
        }

        /* 3. try RDMA read */
        {
            cout << "Client RDMA READ" << endl;
            std::memset(msg_mem.get(), 0, MEM_SIZE);
            BOOST_REQUIRE_EQUAL(string(""), string(reinterpret_cast<char*>(msg_mem.get())));
            ibv_sge sgl[] = {
                {.addr = (uintptr_t)msg_mem.get(), .length = sizeof("1145141919810"),
                 .lkey = msg_mr->lkey},
            };
            ibv_send_wr wr{
                .next = NULL,
                .sg_list = sgl, .num_sge = 1,
                .opcode = IBV_WR_RDMA_READ, /*.send_flags = IBV_SEND_SIGNALED,*/
                .wr = { .rdma = { .remote_addr = (uintptr_t)server_mem.get(),
                                    .rkey = server_msg_mr->rkey } },
            }, *bad_wr;
            BOOST_REQUIRE(!ibv_post_send(client_id->qp, &wr, &bad_wr));
            ibv_wc wc;
            ssize_t poll_spins = 0;
            while (!ibv_poll_cq(client_id->send_cq, 1, &wc)) poll_spins++;
            BOOST_CHECK_EQUAL(wc.status, IBV_WC_SUCCESS);
            BOOST_CHECK_EQUAL(wc.opcode, IBV_WC_RDMA_READ);
            cout << "client poll spinned for " << poll_spins << " times before non-empty" << endl;
            cout << "client polled read " << (int)wc.opcode << " " << wc.vendor_err
                << endl;
            cout << endl;
            BOOST_CHECK_EQUAL(string("1145141919810"), string(reinterpret_cast<char*>(msg_mem.get())));
        }

        /* 4. try RDAM write */
        {
            cout << "Client RDMA WRITE" << endl;
            std::memcpy(msg_mem.get(), "sometext", sizeof("sometext"));
            ibv_sge sgl[] = {
                {.addr = (uintptr_t)msg_mem.get(), .length = sizeof("sometext"),
                 .lkey = msg_mr->lkey},
            };
            ibv_send_wr wr{
                .next = NULL,
                .sg_list = sgl, .num_sge = 1,
                .opcode = IBV_WR_RDMA_WRITE,
                .wr={.rdma={ .remote_addr = (uintptr_t)server_msg_mr->addr,
                             .rkey = server_msg_mr->rkey }},
            }, *bad_wr;
            BOOST_REQUIRE(!ibv_post_send(client_id->qp, &wr, &bad_wr));
            ibv_wc wc;
            while (!ibv_poll_cq(client_id->send_cq, 1, &wc)) ;
            BOOST_CHECK_EQUAL(wc.status, IBV_WC_SUCCESS);
            BOOST_CHECK_EQUAL(wc.opcode, IBV_WC_RDMA_WRITE);
            BOOST_CHECK_EQUAL(string("sometext"), string(reinterpret_cast<char*>(server_mem.get())));
            cout << endl;
        }

        is_client_done = true;
    });

    __client_thread.join();
    __server_thread.join();
}
