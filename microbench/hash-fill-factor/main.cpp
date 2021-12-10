/**
 * @file
 * Test out hash fill factor
 */
#include <iostream>
#include <vector>
#include <filesystem>
#include "ycsb.h"
#include "ycsb_parser.hpp"


int main(const int argc, const char **argv)
{
    namespace yp = smdsbz::ycsb_parser;

    auto workload = std::filesystem::path(YCSB_WORKLOAD_DIR) / "workloada";
    yp::dump_load(YCSB_BIN,
        {{"workload", workload.string()}, {"fieldcount", "1"}},
        "load.txt");

    yp::trace trace;
    yp::parse("load.txt", trace, /*with_value*/false);
    for (const auto &t : trace) {
        std::cout << t << std::endl;
    }

    // TODO: my hash table implementation

    return 0;
}
