/**
 * @file client.cpp
 *
 * Client-side (firing instance) of our perf test
 */

#include <boost/program_options.hpp>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <filesystem>
#include <stdexcept>
#include <thread>
#include <vector>
#include <tuple>
#include <chrono>
#include <random>
#include <arpa/inet.h>
#include <rdma/rdma_cma.h>
#include "common/defer.hpp"
#include "common/size_literals.hpp"

using namespace std;


int main(const int argc, const char **argv)
{
    /* create client stub */
    rdma_addrinfo *addrinfo;
    rdma_addrinfo addr_hint{
        .ai_port_space = RDMA_PS_TCP
    };
    if (rdma_getaddrinfo(/*TODO: server addr*/"192.168.2.246", "9810",
            &addr_hint, &addrinfo))
        throw std::runtime_error("rdma_getaddrinfo()");
    defer([&] { rdma_freeaddrinfo(addrinfo); });
    rdma_cm_id *client_id;
    ibv_qp_init_attr init_attr{
        .cap = { .max_send_wr = 16, .max_recv_wr = 16,
                    .max_send_sge = 16, .max_recv_sge = 16,
                    .max_inline_data = 512 },
        .qp_type = IBV_QPT_RC,
        .sq_sig_all = 0
    };
    if (rdma_create_ep(&client_id, addrinfo, NULL, &init_attr))
        throw std::runtime_error("rdma_create_ep()");
    defer([&] { rdma_destroy_ep(client_id); });

    /* connect to server */
    /* delete previous connection info first */
    if (filesystem::exists("server_mr.txt"))
        filesystem::remove("server_mr.txt");
    if (rdma_connect(client_id, NULL))
        throw std::runtime_error("rdma_connect()");
    std::cout << "connected to " << inet_ntoa(client_id->route.addr.dst_sin.sin_addr) << std::endl;
    defer([&] { rdma_disconnect(client_id); });
    /* get server mr key */
    ibv_mr server_mr;
    {
        /* NOTE: wait for the server to register PMem to RNIC, then write down
            its key. It's 256G, takes some time */
        std::cout << "waiting for server to publish mr" << std::endl;
        while (!filesystem::exists("server_mr.txt"))
            this_thread::sleep_for(.5s);
        ifstream f("./server_mr.txt");
        uintptr_t addr;
        f >> addr >> server_mr.length >> server_mr.rkey;
        server_mr.addr = (void*)addr;
    }
    std::cout << "server_mr: addr " << (uintptr_t)server_mr.addr
        << " length " << server_mr.length
        << " rkey " << server_mr.rkey << std::endl;

    /* register a recv buffer */
    auto recv_buffer = std::make_unique<uint8_t[]>(1_M);
    ibv_mr *local_mr;
    if (local_mr = ibv_reg_mr(client_id->pd, recv_buffer.get(), 1_M,
            IBV_ACCESS_LOCAL_WRITE); !local_mr)
        throw std::runtime_error(string("ibv_reg_mr(): ") + std::strerror(errno));
    defer([&] { ibv_dereg_mr(local_mr); });

#if 0
    /* test - try READ */
    {
        ibv_sge sgl[] = {
            { .addr = (uintptr_t)local_mr->addr, .length = 128,
                .lkey = local_mr->lkey },
        };
        ibv_send_wr wr{
            .next = NULL,
            .sg_list = sgl, .num_sge = 1,
            .opcode = IBV_WR_RDMA_READ, .send_flags = IBV_SEND_SIGNALED,
            .wr = { .rdma = { .remote_addr = (uintptr_t)server_mr.addr,
                                .rkey = server_mr.rkey } },
        }, *bad_wr;
        if (ibv_post_send(client_id->qp, &wr, &bad_wr))
            throw std::runtime_error("ibv_post_send()");
        ibv_wc wc;
        while (!ibv_poll_cq(client_id->send_cq, 1, &wc)) ;
        if (wc.status != IBV_WC_SUCCESS)
            throw std::runtime_error(string("READ not successful ") + to_string(wc.status));
    }
    {
        ifstream f("./payload.tmp", std::ios::binary);
        if (!f)
            throw std::runtime_error("failed to open ./payload.tmp");
        auto payload = std::make_unique<uint8_t[]>(128);
        if (!f.read((char*)payload.get(), 128))
            throw std::runtime_error(string("f.read() "));
        if (std::memcmp(local_mr->addr, payload.get(), 128)) {
            std::cerr << "Read otherwise, diffing (truth / read)" << std::endl;
            for (size_t i = 0; i < 128 / 32; ++i) {
                std::cerr << std::hex
                    << ((uint32_t*)payload.get())[i] << " | "
                    << ((uint32_t*)local_mr->addr)[i]
                    << std::dec << std::endl;
            }
            throw std::runtime_error("Read something else");
        }
    }
#endif

    using test_vector_t = vector<tuple<
        /*length*/uint32_t,
        /*total interval*/std::chrono::duration<double>>>;

    vector<tuple</*length*/uint32_t,
                /*total interval*/std::chrono::duration<double>>>
            test_params{
        {32_B, {}}, {64_B, {}},/*PCIe Lane*/
        {128_B, {}}, {256_B, {}},/*XPLine*/
        {512_B, {}}, {1_K, {}}, {2_K, {}}, {4_K, {}},
        {8_K, {}}, {16_K, {}}, {32_K, {}},
    };
    const size_t TEST_ROUNDS = 1e6;

    auto run_test = [&](
        ibv_send_wr &wr,    ///< tested op, where the first op will be parameterized
        test_vector_t &test_params, ///< test vector & output
        const string &type = "rdma" ///< type of test: "rdma", "cas success" (mostly success), "cas fail"
    ) {
        const bool type_rdma = type == "rdma";
        const bool type_cas = type.starts_with("cas");
        if (!(type_rdma || type_cas))
            throw std::invalid_argument("unknown test type");
        const bool type_cas_success = type == "cas success";
        ibv_send_wr *bad_wr;

        for (auto &[test_iosize, test_run] : test_params) {
            /* ignored for CAS, since all CAS is 8B */
            wr.sg_list[0].length = test_iosize;
            /* generate test sequence */
            vector<uintptr_t> addrs(TEST_ROUNDS);
            {
                std::mt19937 gen(std::random_device{}());
                std::uniform_int_distribution<uintptr_t> distrib(
                    0, server_mr.length / test_iosize - 1);
                for (auto &a : addrs)
                    a = (uintptr_t)server_mr.addr + distrib(gen) * test_iosize;
            }

            /* preload target memory for CAS, where truth value is addr value */
            if (type_cas) {
                ibv_sge sgl[] = {
                    { .addr = (uintptr_t)local_mr->addr, .length = 8, .lkey = local_mr->lkey },
                };
                ibv_send_wr wr{
                    .next = NULL,
                    .sg_list = sgl, .num_sge = 1,
                    .opcode = IBV_WR_RDMA_WRITE, .send_flags = IBV_SEND_SIGNALED,
                    .wr = { .rdma = { .rkey = server_mr.rkey } },
                };
                for (const auto &a : addrs) {
                    wr.wr.rdma.remote_addr = a;
                    *(uint64_t*)local_mr->addr = type_cas_success ? a : a+1;
                    if (ibv_post_send(client_id->qp, &wr, &bad_wr))
                        throw std::runtime_error("ibv_post_send()");
                    ibv_wc wc;
                    while (!ibv_poll_cq(client_id->send_cq, 1, &wc)) ;
                    if (wc.status != IBV_WC_SUCCESS)
                        throw std::runtime_error("RDMA failed");
                }
            }

            /* batch timed test */
            ibv_wc wc;
            const auto start = std::chrono::steady_clock::now();
            for (const auto &a : addrs) {
                if (type_rdma) {
                    wr.wr.rdma.remote_addr = a;
                }
                if (type_cas) {
                    wr.wr.atomic.remote_addr = a;
                    wr.wr.atomic.compare_add = a;
                    wr.wr.atomic.swap = a+114514;
                }

                if (ibv_post_send(client_id->qp, &wr, &bad_wr))
                    throw std::runtime_error("ibv_post_send()");
                while (!ibv_poll_cq(client_id->send_cq, 1, &wc)) ;

                /* NOTE: one may use the returned value of CAS to determine
                    whether the CAS was successful, we won't bother that here,
                    as long as the RDMA op is carried out we can measure perf. */
                if (wc.status != IBV_WC_SUCCESS)
                    throw std::runtime_error("RDMA failed");
                if (type_cas && !type_cas_success && *(uint64_t*)wr.sg_list[0].addr != a+1)
                    throw std::runtime_error("RDMA CAS fail value");
            }
            const auto end = std::chrono::steady_clock::now();
            test_run = end - start;
        }
    };

    auto print_result = [&](const test_vector_t &test_params)
    {
        std::cout << std::left
            << std::setw(12) << "length"
            << std::setw(12) << "total(s)"
            << std::setw(12) << "avg(us)"
            << std::setw(16) << "IOPS(Mop/s)"
            << std::setw(20) << "throughput(MB/s)"
            << "\n" << string(12+12+12+16+20, '-')
            << std::endl;
        for (const auto &[test_iosize, test_run] : test_params)
            std::cout << std::left
                << std::setw(12) << to_human_readable(test_iosize)
                << std::setw(12) << test_run.count()
                << std::setw(12) << (1e6 * test_run / TEST_ROUNDS).count()
                << std::setw(16) << (double)TEST_ROUNDS / test_run.count() / 1e6
                << std::setw(20) << (double)test_iosize * TEST_ROUNDS / test_run.count() / 1_M
                << std::endl;
        std::cout << std::right;
    };

    /* test RDMA Write */
    if (1) {
        std::cout << "\ntesting RDMA Write ..." << std::endl;

        ibv_sge sgl[] = {
            { .addr = (uintptr_t)local_mr->addr, .lkey = local_mr->lkey },
        };
        ibv_send_wr wr{
            .next = NULL,
            .sg_list = sgl, .num_sge = 1,
            .opcode = IBV_WR_RDMA_WRITE, .send_flags = IBV_SEND_SIGNALED,
            .wr = { .rdma = { .rkey = server_mr.rkey } },
        };

        run_test(wr, test_params);
        print_result(test_params);
    }

    /* test RDMA Write with APM */
    if (1) {
        std::cout << "\ntesting RDMA Write with APM ..." << std::endl;

        ibv_sge sgl[] = {
            { .addr = (uintptr_t)local_mr->addr, .lkey = local_mr->lkey },
            { .addr = (uintptr_t)local_mr->addr + local_mr->length - 1, .length = 1, .lkey = local_mr->lkey },
        };
        ibv_send_wr __wr_persist{
            .next = NULL,
            .sg_list = sgl+1, .num_sge = 1,
            .opcode = IBV_WR_RDMA_READ, .send_flags = IBV_SEND_SIGNALED,
            .wr = { .rdma = { .remote_addr = (uintptr_t)server_mr.addr,
                                .rkey = server_mr.rkey } },
        }, wr{
            .next = &__wr_persist,
            .sg_list = sgl, .num_sge = 1,
            .opcode = IBV_WR_RDMA_WRITE,
            .wr = { .rdma = { .rkey = server_mr.rkey } },
        };

        run_test(wr, test_params);
        print_result(test_params);
    }

    /* test RDMA Read */
    if (1) {
        std::cout << "\ntesting RDMA Read ..." << std::endl;

        ibv_sge sgl[] = {
            { .addr = (uintptr_t)local_mr->addr, .lkey = local_mr->lkey },
        };
        ibv_send_wr wr{
            .next = NULL,
            .sg_list = sgl, .num_sge = 1,
            .opcode = IBV_WR_RDMA_READ, .send_flags = IBV_SEND_SIGNALED,
            .wr = { .rdma = { .rkey = server_mr.rkey } },
        };

        run_test(wr, test_params);
        print_result(test_params);
    }

    /* test RDMA CAS */
    if (1) {
        std::cout << "\ntest RDMA CAS ..." << std::endl;

        ibv_sge sgl[] = {
            { .addr = (uintptr_t)local_mr->addr, .lkey = local_mr->lkey },
        };
        ibv_send_wr wr{
            .next = NULL,
            .sg_list = sgl, .num_sge = 1,
            .opcode = IBV_WR_ATOMIC_CMP_AND_SWP, .send_flags = IBV_SEND_SIGNALED,
            .wr = { .atomic = { .rkey = server_mr.rkey } },
        };

        std::cout << "  test success ..." << std::endl;
        run_test(wr, test_params, /*type*/"cas success");
        print_result(test_params);

        std::cout << "  test fail ..." << std::endl;
        run_test(wr, test_params, /*type*/"cas fail");
        print_result(test_params);
    }


    std::cout << "client ended, you may now close server" << std::endl;
    return 0;
}
