/**
 * @file server.cpp
 *
 * Server-side (target instance) of our perf test
 */

#include <boost/program_options.hpp>
#include <iostream>
#include <cstdlib>
#include <fstream>
#include <filesystem>
#include <stdexcept>
#include <libpmem.h>
#include <rdma/rdma_cma.h>
#include <arpa/inet.h>
#include "misc/numa.hpp"
#include "misc/ddio.hpp"
#include "common/defer.hpp"
#include "common/size_literals.hpp"

using namespace std;


int main(const int argc, const char **argv)
{
    /* program arguments */
    std::string pmem_dev;

    namespace po = boost::program_options;
    po::options_description opt_desc;
    opt_desc.add_options()
        ("pmem-dev", po::value(&pmem_dev), "PMem device name");
    po::variables_map opt_map;
    po::store(po::parse_command_line(argc, argv, opt_desc), opt_map);
    po::notify(opt_map);

    /* map PMem space */
    const auto pmem_dev_path = filesystem::path("/dev/") / pmem_dev;
    if (!filesystem::exists(pmem_dev_path) || !filesystem::is_character_file(pmem_dev_path))
        throw std::invalid_argument("pmem-dev");

    size_t pmem_buffer_size; int is_pmem;
    auto pmem_buffer = pmem_map_file(
        pmem_dev_path.c_str(), /*length=entire file*/0, /*flag*/0,
        /*mode*/0, &pmem_buffer_size, &is_pmem
    );
    if (!pmem_buffer)
        throw std::runtime_error(string("pmem_map_file(): ") + std::strerror(errno));
    defer([&] { pmem_unmap(pmem_buffer, pmem_buffer_size); });
    if (!is_pmem)
        throw std::runtime_error("not PMem");
    if ((uintptr_t)pmem_buffer % 2_M && (uintptr_t)pmem_buffer % 4_K)
        throw std::runtime_error("mapped PMem not aligned");
    std::cout << "size of mapped PMem file is " << to_human_readable(pmem_buffer_size) << std::endl;

#if 0
    /* fill it with something to test */
    std::system("dd if=/dev/urandom of=./payload.tmp bs=128 count=1");
    {
        ifstream f("./payload.tmp", std::ios::binary);
        if (!f)
            throw std::runtime_error("failed to open ./payload.tmp");
        if (!f.read((char*)pmem_buffer, 128))
            throw std::runtime_error("f.read()");
        pmem_persist(pmem_buffer, 128);
    }
    std::cout << "payload content:" << std::endl;
    for (size_t i = 0; i < 128 / 32; ++i) {
        std::cout << std::hex << ((uint32_t*)pmem_buffer)[i]
            << std::dec << std::endl;
    }
#endif

    /* init RNIC */
    int num_rnic_devices;
    auto rnic_devices = rdma_get_devices(&num_rnic_devices);
    defer([&] { rdma_free_devices(rnic_devices); });
    if (!num_rnic_devices)
        throw std::runtime_error("get an RNIC first, dude");
    auto rnic_chosen = gestalt::misc::numa::choose_rnic_on_same_numa(
        pmem_dev.c_str(), rnic_devices, num_rnic_devices);
    if (!rnic_chosen) {
        std::cerr << "cannot find a matching RNIC on the same NUMA, "
            << "default to the first RNIC listed!" << std::endl;
        rnic_chosen = rnic_devices[0]->device;
    }
    std::cout << "RNIC chosen is " << rnic_chosen->name << std::endl;
    /* RNIC DDIO configure */
    auto ddio_guard(gestalt::misc::ddio::scope_guard::from_rnic(rnic_chosen->name));

    /* register PMem to RNIC to be RW-ready */

    /* start RDMACM service */
    rdma_addrinfo *server_addrinfo,
        server_addrhint{
            .ai_flags = RAI_PASSIVE, .ai_port_space = RDMA_PS_TCP
        };
    if (rdma_getaddrinfo(/*TODO: get from rnic*/"192.168.2.246", "9810",
            &server_addrhint, &server_addrinfo))
        throw std::runtime_error(string("rdma_getaddrinfo(): ") + std::strerror(errno));
    defer([&] { rdma_freeaddrinfo(server_addrinfo); });
    rdma_cm_id *server_id;
    ibv_qp_init_attr init_attr{
        .cap = { .max_send_wr = 16, .max_recv_wr = 16,
                    .max_send_sge = 16, .max_recv_sge = 16,
                    .max_inline_data = 512 },
        .qp_type = IBV_QPT_RC,
        .sq_sig_all = 0
    };
    if (rdma_create_ep(&server_id, server_addrinfo, /*qp*/NULL, &init_attr))
        throw std::runtime_error(string("rdma_create_ep(): ") + std::strerror(errno));
    defer([&] { rdma_destroy_ep(server_id); });
    if (rdma_listen(server_id, 0))
        throw std::runtime_error(string("rdma_listen(): ") + std::strerror(errno));

    /* listen for incomming connections */
    rdma_cm_id *connected_id;
    if (rdma_get_request(server_id, &connected_id))
        throw std::runtime_error(string("rdma_get_reqeust(): ") + std::strerror(errno));
    defer([&] { rdma_destroy_ep(connected_id); });
    if (rdma_accept(connected_id, NULL))
        throw std::runtime_error(string("rdma_accept(): ") + std::strerror(errno));
    defer([&] { rdma_disconnect(connected_id); });
    std::cout << "accepted connection from "
        /* NOTE: and yes, `dst_sin` is the received address, the other end,
            whilst `src_sin` is ours */
        << inet_ntoa(connected_id->route.addr.dst_sin.sin_addr)
        << std::endl;
    /* register PMem */
    ibv_mr *mr;
    /* NOTE: IBV_ACCESS_ON_DEMAND is required for RPMem-ing to FSDAX, try
        eliminate mandatory page fault with DEVDAX */
    if (mr = ibv_reg_mr(connected_id->pd, pmem_buffer, pmem_buffer_size,
            /*IBV_ACCESS_ON_DEMAND |*/ IBV_ACCESS_LOCAL_WRITE |
            IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE |
            IBV_ACCESS_REMOTE_ATOMIC); !mr)
        throw std::runtime_error(string("ibv_reg_mr() ") + std::strerror(errno));
    defer([&] { ibv_dereg_mr(mr); });
    std::cout << "server_mr: addr " << (uintptr_t)mr->addr
        << " length " << mr->length
        << " rkey " << mr->rkey << std::endl;
    {
        ofstream f("./server_mr.txt");
        f << (uintptr_t)mr->addr << " " << mr->length << " " << mr->rkey;
    }

    /* CMBK: halt until interrupt, an RDMA Send from initiator will indicate
        termination */
    std::cout << "Enter 'q' to terminate [q] " << std::flush;
    char q;
    do {
        std::cin >> q;
    } while (q != 'q');

    return 0;
}
