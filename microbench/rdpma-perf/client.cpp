/**
 * @file client.cpp
 *
 * Client-side (firing instance) of our perf test
 */

#include <boost/program_options.hpp>
#include <iostream>
#include <stdexcept>
#include <arpa/inet.h>
#include <rdma/rdma_cma.h>
#include "common/defer.hpp"

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

    return 0;
}
