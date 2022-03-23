#include "ycsb_parser.hpp"
#include <sstream>
#include <cstdlib>
#include <regex>
#include <fstream>
#include <stdexcept>
#include <boost/regex.hpp>
#include <boost/log/trivial.hpp>

namespace smdsbz {
namespace ycsb_parser {

namespace {
enum class Stage
{
    LOAD, RUN
};
std::ostream& operator<<(std::ostream& os, const Stage &s)
{
    switch (s) {
    case Stage::LOAD: {
        os << "load";
        break;
    }
    case Stage::RUN: {
        os << "run";
        break;
    }
    default: {
        throw std::invalid_argument("unknown YCSB stage");
    }
    }
    return os;
}

/**
 * execute YCSB
 */
void run(
    const path &ycsb, const Stage stage, const ycsb_args &args,
    const path& dumppath
) {
    ostringstream cli;
    if (!filesystem::is_regular_file(ycsb))
        throw invalid_argument("YCSB executable not found");
    auto workload_iter = args.find("workload");
    if (workload_iter == args.cend())
        throw invalid_argument("YCSB workload must be specified");
    if (!filesystem::is_regular_file(workload_iter->second))
        throw invalid_argument("YCSB workload spec file not found");

    /* required cli */
    cli << ycsb << " "
        << stage << " basic "
        << "-P " << workload_iter->second;

    /* optional args */
    for (auto [k, v] : args) {
        if (k == "workload")
            continue;
        cli << " -p " << k << "=" << v;
    }
    // BOOST_LOG_TRIVIAL(debug) << "cli: " << cli.str();

    /* generate to local temporary file, copy it to wanted location later with
        a single system call, so it works better with NFS */
    const auto tmppath = filesystem::path("/tmp/smdsbz-ycsb-parser-run.tmp");
    cli << " > " << tmppath;

    /* run command */
    std::system(cli.str().c_str());

    /* move to wanted location, with one system call */
    filesystem::copy_file(tmppath, dumppath, filesystem::copy_options::overwrite_existing);
    filesystem::remove(tmppath);
}
}   /* anonymous-namespace */

void dump_load(
    const path &ycsb, const ycsb_args &args,
    const path &outpath
) {
    run(ycsb, Stage::LOAD, args, outpath);
}

void dump_run(
    const path &ycsb, const ycsb_args &args,
    const path &outpath
) {
    run(ycsb, Stage::RUN, args, outpath);
}


namespace {
/**
 * __Submatches__
 *
 * 1. operation
 * 2. table name
 * 3. object key
 * 4. fields
 */
const auto entry_regex = std::regex("(INSERT|READ|UPDATE) (\\S+) (\\S+) \\[ (.+?) ?\\]");
}   /* anonymous-namespace */

void parse(
    const path &dumppath,
    vector<ycsb_entry> &out,
    bool with_value
) {
    out.clear();
    if (out.capacity() < 1<<10)
        out.reserve(1<<10);

    fstream f(dumppath, ios_base::in | ios_base::binary);
    for (string l; getline(f, l, '\n'); ) {
        // BOOST_LOG_TRIVIAL(debug) << l;
        smatch m;
        regex_match(l, m, entry_regex);
        if (m.empty())
            continue;
        // BOOST_LOG_TRIVIAL(debug) << "op: " << m[1].str();
        // BOOST_LOG_TRIVIAL(debug) << "table: " << m[2].str();
        // BOOST_LOG_TRIVIAL(debug) << "okey: " << m[3].str();
        // BOOST_LOG_TRIVIAL(debug) << "fields: " << m[4].str();
        ycsb_entry e;
        {
            const auto op = m[1].str();
            if (op == "INSERT")
                e.op = ycsb_entry::Op::INSERT;
            else if (op == "READ")
                e.op = ycsb_entry::Op::READ;
            else if (op == "UPDATE")
                e.op = ycsb_entry::Op::UPDATE;
            else {
                BOOST_LOG_TRIVIAL(warning) << "unknown YCSB op " << op
                    << ", ignored";
            }
        }
        e.table = m[2].str();
        e.okey = m[3].str();
        /* TODO: parse fields */
        out.push_back(std::move(e));
    }
}

}   /* namespace ycsb_parser */
}   /* namespace smdsbz */


std::ostream& operator<<(std::ostream& os, const smdsbz::ycsb_parser::ycsb_entry &e)
{
    using namespace smdsbz::ycsb_parser;
    os << "<";
    switch (e.op) {
    case ycsb_entry::Op::INSERT: {
        os << "INSERT";
        break;
    }
    case ycsb_entry::Op::READ: {
        os << "READ";
        break;
    }
    case ycsb_entry::Op::UPDATE: {
        os << "UPDATE";
        break;
    }
    default: {
        os << "unknown";
        break;
    }
    }
    os << " " << e.table << "/" << e.okey;
    os << ">";
    return os;
}
