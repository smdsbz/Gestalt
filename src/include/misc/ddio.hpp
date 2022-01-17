/**
 * @file ddio.hpp
 * Handy helpers for DDIO configuring
 */

#pragma once

#include <boost/core/noncopyable.hpp>


namespace gestalt {
namespace misc {
namespace ddio {

using namespace std;

/**
 * Configuring and restoring Intel DDIO setting in C++ RAII style
 */
class scope_guard : boost::noncopyable {
    string pci_root;
    /**
     * If the passed in device is not actual PCI hardware, e.g. software emulated,
     * the scope guard should do nothing.
     */
    bool do_nothing;
    uint8_t original_perfctrlsts;

    /* constructors */
public:
    scope_guard() = default;
    ~scope_guard();
    scope_guard(scope_guard &&other);
    /**
     * Construct a scope guard based on RNIC
     * @param dev name of ib_device
     * @return movable scope guard
     */
    static scope_guard from_rnic(const char *dev);
};  /* class scope_guard */

}   /* namespace ddio */
}   /* namespace misc */
}   /* namespace gestalt */
