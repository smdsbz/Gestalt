/**
 * @file
 * A minimal YCSB parser
 */

#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <filesystem>
#include <memory>
#include <iostream>


namespace smdsbz {
namespace ycsb_parser {

using namespace std;

/**
 * name - value, value can be `unsigned` or string
 * @sa https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties
 */
using ycsb_args = unordered_map<string, string>;
using filesystem::path;

/**
 * Run YCSB load stage and dump output to file
 * @param[in] ycsb Path to YCSB executable
 * @param[in] args YCSB parameters
 * @param[in] outpath Path to dump load output
 */
void dump_load(
    const path &ycsb, const ycsb_args &args,
    const path &outpath
);

/**
 * Run YCSB run stage and dump output to file
 * @param[in] ycsb Path to YCSB executable
 * @param[in] args YCSB parameters
 * @param[in] outpath Path to dump load output
 */
void dump_run(
    const path &ycsb, const ycsb_args &args,
    const path &outpath
);

/**
 * YCSB Operation
 */
struct ycsb_entry {
    enum class Op {
        INSERT,
        READ,
        UPDATE
    };
    Op op;  ///< operation
    string table;  ///< table name
    string okey;   ///< object key
    vector<shared_ptr<uint8_t[]>> fields;   ///< object value
};

using trace = vector<ycsb_entry>;

/**
 * Parse YCSB output
 * @param[in] dumppath Path to dumped YCSB output
 * @param[out] out Parsed native structure
 * @param[in] with_data If parse field data, may consume a lot of RAM, defaults
 *      to true.
 */
void parse(
    const path &dumppath,
    trace &out,
    bool with_value = true
);

}   /* namespace smdsbz */
}   /* namespace ycsb_parser */


std::ostream& operator<<(std::ostream& os, const smdsbz::ycsb_parser::ycsb_entry &e);
