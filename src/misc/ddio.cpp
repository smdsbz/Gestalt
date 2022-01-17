#include <filesystem>
#include <regex>
#include <cstdlib>
#include <fstream>
#include <ostream>
#include "misc/ddio.hpp"
#include "common/defer.hpp"


namespace gestalt {
namespace misc {
namespace ddio {

using namespace std;

namespace {
string to_hex_str(int d)
{
    ostringstream s;
    s << std::hex << d;
    return s.str();
}
}

scope_guard::scope_guard(scope_guard &&other)
{
    pci_root = other.pci_root;
    do_nothing = other.do_nothing;
    original_perfctrlsts = other.original_perfctrlsts;
}

scope_guard::~scope_guard()
{
    if (do_nothing)
        return;
    std::system((string("setpci -s ") + pci_root + " 180.b=" +
        to_hex_str(original_perfctrlsts)).c_str());
}


scope_guard scope_guard::from_rnic(const char *dev)
{
    auto g = scope_guard();

    /* find out dev's PCI root port (domain:bus:device.func) */
    const auto class_path = filesystem::path("/sys/class/infiniband/") / dev;
    const auto device_path = filesystem::read_symlink(class_path);
    if (!device_path.string().starts_with("../../devices/pci")) {
        g.do_nothing = true;
        return g;
    }

    /**
     * __Submatch__
     * 1. pci root
     */
    const auto pci_root_regex = regex(
        "^\\.\\./\\.\\./devices/pci[\\da-f]{4}:[\\da-f]{2}/"
        "([\\da-f]{4}:[\\da-f]{2}:[\\da-f]{2}\\.[\\da-f])"
    );
    smatch m;
    /* NOTE: std::regex_match just hate rval as input, make it happy :) */
    const auto device_path_str = device_path.string();
    regex_search(device_path_str, m, pci_root_regex);
    if (m.empty()) {
        g.do_nothing = true;
        return g;
    }
    g.pci_root = m[1];

    /* save current state */
    {
        filesystem::create_directory("/tmp/gestalt");
        const char *TMPFILE = "/tmp/gestalt/ddio.out";
        std::system((string("setpci -s ") + g.pci_root + " 180.b"
            + " > " + TMPFILE).c_str());
        defer([&]{ filesystem::remove(TMPFILE); });
        ifstream f(TMPFILE);
        /* NOTE: as type of `original_perfctrlsts` is uint8_t, directly >> to it
           will read as char ... */
        int r;
        f >> std::hex >> r;
        g.original_perfctrlsts = r;
    }
    if (g.original_perfctrlsts == 0xff) {
        g.do_nothing = true;
        return g;
    }

    /* turn DDIO off */
    int r = std::system((string("setpci -s ") + g.pci_root + " 180.b=" +
        to_hex_str(g.original_perfctrlsts & 0b01111111)).c_str());
    if (r) {
        g.do_nothing = true;
        return g;
    }

    g.do_nothing = false;
    return g;
}

}   /* namespace ddio */
}   /* namespace misc */
}   /* namespace gestalt */
