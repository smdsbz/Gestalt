#include <cstdlib>
#include <vector>
#include <sstream>
#include <fstream>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#ifdef GET_NUMA_WITH_NDCTL
#include <ndctl/libndctl.h>
#endif
#include <boost/property_tree/json_parser.hpp>
#include <rdma/rdma_cma.h>
#include "misc/numa.hpp"
#include "common/defer.hpp"


namespace gestalt {
namespace misc {
namespace numa {

using namespace std;


int get_numa_node(const ibv_device *dev)
{
    auto numa_file = filesystem::path(dev->ibdev_path) / "device" / "numa_node";
    if (!filesystem::exists(numa_file))
        return -1;
    ifstream nf(numa_file);
    int numa_id;
    nf >> numa_id;
    return numa_id;
}

ibv_device *choose_rnic_on_same_numa(
    const char *pmem_dev,
    ibv_context **devices, size_t num_devices
) {
    int numa = -1;

#ifdef GET_NUMA_WITH_NDCTL
    /* native programatic approach with `libndctl`
       iterate through all bus -> region -> namespace and find out our PMem
       dev's NUMA  */
    // DEBUG: ndctl_namespace_get_block_device() always return empty string
    {
        ndctl_ctx *ndctx;
        if (ndctl_new(&ndctx))
            throw std::runtime_error("ndctl_new");
        defer(ndctx, ndctl_unref);

        ndctl_bus *bus;
        ndctl_region *reg;
        ndctl_namespace *ns;
        bool found = false;
        ndctl_bus_foreach(ndctx, bus) {
            ndctl_region_foreach(bus, reg) {
                ndctl_namespace_foreach(reg, ns) {
                    const auto &in = ndctl_namespace_get_block_device(ns);
                    if (!strcmp(pmem_dev, in))
                        found = true;
                    if (found)
                        break;
                }
                if (found)
                    break;
            }
            if (found)
                break;
        }
        if (!found)
            throw std::runtime_error(string("no such device: ") + pmem_dev);

        numa = ndctl_region_get_numa_node(reg);
    }
#endif

    /* parse from `ndctl` approach */
    {
        namespace pt = boost::property_tree;

        /* get `ndctl` output */
        const auto outdir = filesystem::path("/tmp/gestalt/");
        filesystem::create_directory(outdir);
        const auto out = outdir / "ndctl_out.json";
        std::system((string("echo -n '{\"data\":' > ") + out.string()).c_str());
        std::system((string("ndctl list -v >> ") + out.string()).c_str());
        std::system((string("echo -n '}' >> ") + out.string()).c_str());

        ifstream ndctl_dump(out);
        pt::ptree tree;
        pt::read_json(ndctl_dump, tree);

        /* find our device's NUMA */
        for (const auto &[i, d] : tree.get_child("data")) {
            const auto &dn = d.get<std::string>("blockdev");
            if (dn != pmem_dev)
                continue;
            numa = d.get<int>("numa_node");
            break;
        }
    }

    /* iterate through RNIC devices */
    for (int i = 0; i < num_devices; ++i) {
        const auto &dev = devices[i]->device;
        if (get_numa_node(dev) == numa)
            return dev;
    }

    return nullptr;
}


}   /* namespace numa */
}   /* namespace misc */
}   /* namespace gestalt */
