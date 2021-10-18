#define BOOST_TEST_MODULE rdma hello world
#include <boost/test/unit_test.hpp>

#include <iostream>
using std::cout, std::endl, std::flush;
#include <functional>
template<typename T> using deleted_unique_ptr = std::unique_ptr<T, std::function<void(T*)>>;
#define CONCAT(a, b) a ## b
#define __defer_impl(ptr, dfn, l)                               \
    std::unique_ptr<std::remove_pointer<decltype(ptr)>::type,   \
                    std::function<void(decltype(ptr))>>         \
    CONCAT(__dtor, l) (ptr, [](decltype(ptr) p) { dfn(p); })
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
    std::cout << /*"(" << typeid(V).name() << ")" <<*/ #V "=" << V << std::endl;  \
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
 * DEPRECATED: Seems the timing does not work well, something requires the server
 * thread and client thread both active at the same time. Just use the fully-
 * threaded version {@see rdma_hello_world_threaded} down below.
 */
// BOOST_AUTO_TEST_CASE(rdma_hello_world) {
//     /* get link layer routing info */
//     rdma_addrinfo *server_addrinfo;
//     rdma_addrinfo server_addr_hint{
//         .ai_flags = RAI_PASSIVE
//     };
//     BOOST_REQUIRE(!rdma_getaddrinfo(PLAYGROUND_RNIC_IP, PLAYGROUND_RNIC_PORT, &server_addr_hint, &server_addrinfo));
//     defer(server_addrinfo, rdma_freeaddrinfo);
//     /* create server stub */
//     rdma_cm_id *server_id;
//     ibv_qp_init_attr server_init_attr{
//         .cap = {.max_send_wr = 1, .max_recv_wr = 1,
//                 .max_send_sge = 1, .max_recv_sge = 1,
//                 .max_inline_data = 512},
//         .sq_sig_all = 1
//     };
//     BOOST_REQUIRE(!rdma_create_ep(&server_id, server_addrinfo, NULL, &server_init_attr));
//     defer(server_id, rdma_destroy_ep);
//     auto server_msg_mem = std::make_unique<uint8_t[]>(512);
//     auto server_msg_mr = rdma_reg_msgs(server_id, server_msg_mem.get(), 512);
//     BOOST_REQUIRE(server_msg_mr);
//     defer(server_msg_mr, rdma_dereg_mr);
//     cout << "Server state before connect" << endl;
//     DVAR(server_id->verbs);
//     DVAR(server_id->send_cq); DVAR(server_id->recv_cq); DVAR(server_id->srq);
//     DVAR(server_id->qp_type);
//     cout << endl;
//     /* start server listening for incoming connections */
//     BOOST_REQUIRE(!rdma_listen(server_id, 0));
//     rdma_cm_id *server_client_connection;
//     packaged_task<int(void)> __server_accept_task([&] {
//         int r;
//         r = rdma_get_request(server_id, &server_client_connection);
//         if (r) return r;
//         cout << "server got request" << endl;
//         // r = rdma_post_recv(server_client_connection, NULL, server_msg_mem.get(), 512, server_msg_mr);
//         // if (r) return r;
//         // cout << "server posted recv" << endl;
//         r = rdma_accept(server_client_connection, NULL);
//         if (r) return r;
//         cout << "server accepted connection" << endl;
//         return 0;
//     });
//     auto __server_accept_future = __server_accept_task.get_future();
//     thread __server_accept_thread(std::move(__server_accept_task));
//     /* create client stub */
//     rdma_addrinfo *client_addrinfo;
//     rdma_addrinfo client_addr_hint{
//         .ai_port_space = RDMA_PS_TCP
//     };
//     BOOST_REQUIRE(!rdma_getaddrinfo(PLAYGROUND_RNIC_IP, PLAYGROUND_RNIC_PORT, &client_addr_hint, &client_addrinfo));
//     defer(client_addrinfo, rdma_freeaddrinfo);
//     rdma_cm_id *client_id;
//     ibv_qp_init_attr client_init_attr{
//         .cap = {.max_send_wr = 1, .max_recv_wr = 1,
//                 .max_send_sge = 1, .max_recv_sge = 1,
//                 .max_inline_data = 512},
//         .sq_sig_all = 1
//     };
//     BOOST_REQUIRE(!rdma_create_ep(&client_id, client_addrinfo, NULL, &client_init_attr));
//     defer(client_id, rdma_destroy_ep);
//     auto client_msg_mem = std::make_unique<uint8_t[]>(512);
//     auto client_msg_mr = rdma_reg_msgs(client_id, client_msg_mem.get(), 512);
//     BOOST_REQUIRE(client_msg_mr);
//     defer(client_msg_mr, rdma_dereg_mr);
//     // BOOST_REQUIRE(!rdma_post_recv(client_id, NULL, client_msg_mem.get(), 512, client_msg_mr));
//     /* client try connect to server, server accept connection in background thread*/
//     BOOST_TEST_REQUIRE(!rdma_connect(client_id, NULL), std::strerror(errno));
//     cout << "client connected" << endl;
//     defer(client_id, [](rdma_cm_id *p) {
//         BOOST_TEST_CHECK(!rdma_disconnect(p), std::strerror(errno));
//         cout << "client disconnected" << endl;
//     });
//     /* server accept connection */
//     __server_accept_thread.join();
//     BOOST_REQUIRE(!__server_accept_future.get());
//     BOOST_REQUIRE(server_client_connection);
//     defer(server_client_connection, [](rdma_cm_id *p) {
//         BOOST_TEST_CHECK(!rdma_disconnect(p), std::strerror(errno));
//         cout << "server released connection" << endl;
//     });
//     cout << "Server state after connect" << endl;
//     DVAR(server_id->verbs);
//     DVAR(server_id->send_cq); DVAR(server_id->recv_cq); DVAR(server_id->srq);
//     DVAR(server_id->qp_type);
//     cout << endl;
//     /* stopping client */
//     cout << "stopping client" << endl;
//     struct ibv_wc client_wc;
//     while (rdma_get_recv_comp(client_id, &client_wc) == 0);
//     while (rdma_get_send_comp(client_id, &client_wc) == 0);
//     /* stopping server */
//     cout << "stopping server" << endl;
//     struct ibv_wc server_wc;
//     while (rdma_get_recv_comp(server_id, &server_wc) == 0);
//     while (rdma_get_send_comp(server_id, &server_wc) == 0);
// }

/**
 * Steps:
 * 1. Establish connection
 * 2. Client send (librdmacm)
 * 3. Client write (librdmacm) (tested out of thread context)
 */
BOOST_AUTO_TEST_CASE(rdma_hello_world_threaded) {
    cout << "==== Entering rdma_hello_world_threaded ====" << endl;
    constexpr size_t MEM_SIZE = 512;
    auto server_mem = std::make_unique<uint8_t[]>(MEM_SIZE);
    std::memcpy(server_mem.get(), "1145141919810", sizeof("1145141919810")-1);
    ibv_mr *server_msg_mr;

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
            .cap = {.max_send_wr = 1, .max_recv_wr = 1,
                    .max_send_sge = 1, .max_recv_sge = 1,
                    .max_inline_data = 512},
            .qp_type = IBV_QPT_RC,
            .sq_sig_all = 1
        };
        BOOST_REQUIRE(!rdma_create_ep(&server_id, addrinfo, NULL, &init_attr));
        defer(server_id, rdma_destroy_ep);
        cout << "Server state before connect" << endl;
        DVAR(server_id->verbs);
        DVAR(server_id->send_cq); DVAR(server_id->recv_cq); DVAR(server_id->srq);
        DVAR(server_id->qp); DVAR(server_id->qp_type);
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
        cout << "Server (end-point) state after connect" << endl;
        DVAR(server_id->verbs);
        DVAR(server_id->send_cq); DVAR(server_id->recv_cq); DVAR(server_id->srq);
        DVAR(server_id->qp); DVAR(server_id->qp_type);
        DVAR(server_id->pd->handle);
        cout << "Server (agent) state after connect" << endl;
        DVAR(connected_client_id->verbs);
        DVAR(connected_client_id->send_cq); DVAR(connected_client_id->recv_cq); DVAR(connected_client_id->srq);
        DVAR(connected_client_id->qp); DVAR(connected_client_id->qp_type);
        DVAR(connected_client_id->pd->handle);
        cout << endl;
        /* prepare RDMA-enabled memory */
        server_msg_mr = rdma_reg_msgs(connected_client_id, server_mem.get(), MEM_SIZE);
        BOOST_REQUIRE(server_msg_mr);
        defer(server_msg_mr, rdma_dereg_mr);

        /* 2. handle client send */
        // TODO:
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
            .cap = {.max_send_wr = 1, .max_recv_wr = 1,
                    .max_send_sge = 1, .max_recv_sge = 1,
                    .max_inline_data = 512},
            .qp_type = IBV_QPT_RC,
            .sq_sig_all = 1
        };
        BOOST_REQUIRE(!rdma_create_ep(&client_id, addrinfo, NULL, &init_attr));
        defer(client_id, rdma_destroy_ep);
        cout << "Client state after end-point creation" << endl;
        DVAR(client_id->verbs);
        DVAR(client_id->send_cq);  DVAR(client_id->recv_cq); DVAR(client_id->srq);
        DVAR(client_id->qp); DVAR(client_id->qp_type);
        cout << endl;
        /* prepare RDMA-enabled memory */
        auto msg_mem = std::make_unique<uint8_t[]>(MEM_SIZE);
        auto msg_mr = rdma_reg_msgs(client_id, msg_mem.get(), MEM_SIZE);
        BOOST_REQUIRE(msg_mr);
        defer(msg_mr, rdma_dereg_mr);

        /* client try connect to server, server accept connection in background thread*/
        BOOST_TEST_REQUIRE(!rdma_connect(client_id, NULL), std::strerror(errno));
        cout << "client connected" << endl;
        defer(client_id, [](rdma_cm_id *p) {
            BOOST_TEST_CHECK(!rdma_disconnect(p), std::strerror(errno));
            cout << "client disconnected" << endl;
        });

        ///* CMBK: try RDMA read */
        //BOOST_TEST(!rdma_post_read(client_id, NULL, msg_mem.get(), 10, msg_mr, 0, (uint64_t)server_msg_mr->addr, server_msg_mr->rkey));
        //std::this_thread::sleep_for(1s);
        //std::printf("msg_mem.get(): %10s\n", msg_mem.get());

        /* 2. fire client send */
        // TODO:
    });

    __server_thread.join();
    __client_thread.join();
}
