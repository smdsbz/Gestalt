/**
 * @file client.cpp
 *
 * Client-side (firing instance) of our perf test
 */

#include <boost/program_options.hpp>
#include <iostream>
#include <iomanip>
#include <fstream>
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
    if (rdma_connect(client_id, NULL))
        throw std::runtime_error("rdma_connect()");
    std::cout << "connected to " << inet_ntoa(client_id->route.addr.dst_sin.sin_addr) << std::endl;
    defer([&] { rdma_disconnect(client_id); });
    /* get server mr key */
    ibv_mr server_mr;
    {
        /* NOTE: wait for server to write down its key */
        this_thread::sleep_for(3s);
        ifstream f("./server_mr.txt");
        uintptr_t addr;
        f >> addr >> server_mr.rkey;
        server_mr.addr = (void*)addr;
    }
    std::cout << "server_mr: addr " << (uintptr_t)server_mr.addr
        << " rkey " << server_mr.rkey << std::endl;

    /* register a recv buffer */
    auto recv_buffer = std::make_unique<uint8_t[]>(1_M);
    ibv_mr *local_mr;
    if (local_mr = ibv_reg_mr(client_id->pd, recv_buffer.get(), 1_M,
            IBV_ACCESS_LOCAL_WRITE); !local_mr)
        throw std::runtime_error(string("ibv_reg_mr(): ") + std::strerror(errno));
    defer([&] { ibv_dereg_mr(local_mr); });

    if (0) {
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
    }

    /* test RDMA Write */
    {
        std::cout << "testing RDMA Write with APM ..." << std::endl;

        ibv_sge sgl[] = {
            /* parameterize length later */
            { .addr = (uintptr_t)local_mr->addr, .lkey = local_mr->lkey },
            { .addr = (uintptr_t)local_mr->addr/* + local_mr->length - 1*/, .length = 1, .lkey = local_mr->lkey },
        };
        ibv_send_wr __wr_persist{
            .next = NULL,
            .sg_list = sgl+1, .num_sge = 1,
            .opcode = IBV_WR_RDMA_READ, .send_flags = IBV_SEND_SIGNALED,
            .wr = { .rdma = { .remote_addr = (uintptr_t)server_mr.addr,
                                .rkey = server_mr.rkey } },
        }, wr{
            .next = &__wr_persist,
            // .next = NULL,
            .sg_list = sgl, .num_sge = 1,
            .opcode = IBV_WR_RDMA_WRITE, //.send_flags = IBV_SEND_SIGNALED,
            .wr = { .rdma = { .remote_addr = (uintptr_t)server_mr.addr,
                                .rkey = server_mr.rkey } },
        }, *bad_wr;

        const size_t TEST_ROUNDS = 1000000;
        vector<tuple</*length*/uint32_t,
                        /*total interval*/std::chrono::duration<double>>>
                test_params{
            {32, {}}, {64, {}},/*PCIe Lane*/
            {128, {}}, {256, {}},/*XPLine*/
            {512, {}}, {2_K, {}}, {4_K, {}}, {8_K, {}}
        };
        for (auto &t : test_params) {
            sgl[0].length = std::get<0>(t);
            vector<uintptr_t> addrs(TEST_ROUNDS);
            {
                std::mt19937 gen(std::random_device{}());
                std::uniform_int_distribution<uintptr_t> distrib(
                    (uintptr_t)server_mr.addr,
                    (uintptr_t)server_mr.addr + server_mr.length
                );
                for (auto &a : addrs)
                    a = distrib(gen);
            }

            const auto start = std::chrono::steady_clock::now();
            for (const auto &a : addrs) {
                wr.wr.rdma.remote_addr = a;
                if (ibv_post_send(client_id->qp, &wr, &bad_wr))
                    throw std::runtime_error("ibv_post_send()");
                ibv_wc wc;
                while (!ibv_poll_cq(client_id->send_cq, 1, &wc)) ;
                if (wc.status != IBV_WC_SUCCESS)
                    throw std::runtime_error(string("Persist Write failed ") + to_string(wc.status));
            }
            const auto end = std::chrono::steady_clock::now();
            std::get<1>(t) = end - start;

            std::cout << "length " << std::setw(6) << to_human_readable(std::get<0>(t))
                << " total(s) " << std::setw(9) << std::get<1>(t).count()
                << " avg(us) " << std::setw(9) << (1e6 * std::get<1>(t) / TEST_ROUNDS).count()
                << " IOPS(Mop/s) " << std::setw(9) << TEST_ROUNDS / std::get<1>(t).count() / 1e6
                << " throughput(MB/s) " << std::setw(9) << std::get<0>(t) * TEST_ROUNDS / std::get<1>(t).count() / 1e6
                << std::endl;
        }
    }


    std::cout << "client ended, you may now close server" << std::endl;
    return 0;
}
