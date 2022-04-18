/**
 * @file
 * Test out hash fill factor
 */
#include <iostream>
#include <vector>
#include <cstring>
#include <isa-l/crc.h>
#include <filesystem>
#include <cassert>
#include <boost/log/trivial.hpp>
#include "ycsb.h"
#include "ycsb_parser.hpp"
#include "headless_hashtable.hpp"


namespace {
struct __attribute__ ((packed)) entry {
    struct __attribute__ ((packed)) key_type {
        char _d[128];
        key_type() noexcept
        {
            _d[0] = '\0';
        }
        key_type (const char *k)
        {
            if (std::strlen(k) > 127)
                throw std::invalid_argument("key too long");
            std::strcpy(_d, k);
        }
        key_type (const std::string &k)
        {
            if (k.length() > 127)
                throw std::invalid_argument("key too long");
            std::strcpy(_d, k.c_str());
        }
        key_type &operator=(const key_type &that) noexcept
        {
            std::strcpy(_d, that._d);
            return *this;
        }
        inline const char *c_str() const noexcept
        {
            return _d;
        }
        inline bool operator==(const key_type &that) const noexcept
        {
            return std::strcmp(this->_d, that._d) == 0;
        }
        static inline uint32_t hash(const std::string &k) noexcept
        {
            return crc32_iscsi((unsigned char*)k.c_str(), k.length(), 0x11451419);
            return crc32_ieee(0x11451419, (unsigned char*)k.c_str(), k.length());
            return std::hash<std::string>{}(k) & 0xffffffff;
        };
        inline uint32_t hash() const noexcept
        {
            return key_type::hash(std::string(_d));
        }
    } _key;
    inline const key_type &key() const noexcept { return _key; }

    struct __attribute__ ((packed)) value_type {
    } _value;
    inline value_type &value() noexcept { return _value; }

    entry() noexcept : _key(), _value() {}
    entry(const std::string &k, const value_type &v) : _key(k), _value() {}

    inline void invalidate() noexcept
    {
        _key._d[0] = '\0';
    }
    inline bool is_valid() const noexcept
    {
        return _key._d[0] != '\0';
    }
    inline bool is_invalid() const noexcept
    {
        return !this->is_valid();
    }
};
static_assert(sizeof(entry) == 128 + 1/*for empty value struct*/);
}

int main(const int argc, const char **argv)
{
    namespace yp = smdsbz::ycsb_parser;
    auto workload_path = std::filesystem::path(YCSB_WORKLOAD_DIR) / "workloada";
    auto load_dump_path = std::filesystem::path(".") / "load.txt";

    /* unittesting my YCSB parser */
    {
        BOOST_LOG_TRIVIAL(debug) << "unittest: YCSB parser";
        yp::dump_load(YCSB_BIN,
            {{"workload", workload_path.string()}, {"fieldcount", "1"}},
            load_dump_path);

        yp::trace trace;
        yp::parse(load_dump_path, trace, /*with_value*/false);
        // for (const auto &t : trace) {
        //    std::cout << t << std::endl;
        // }
    }

    /* unittesting my hash table implementation */
    {
        BOOST_LOG_TRIVIAL(debug) << "unittest HeadlessHashTable";
        auto arr = std::make_unique<entry[]>(4096);
        gestalt::HeadlessHashTable<entry> hht(arr.get(), 4096);
        /* optional since space is default constructed thus naturally invalid */
        // hht.clear();

        hht["114"] = {"114", {}};
        assert(hht.contains("114"));

        assert(!hht.contains("1919"));
        hht.insert({"1919", {}});
        assert(hht.contains("1919"));

        bool found_114 = false, found_1919 = false;
        for (const auto &e : hht) {
            BOOST_LOG_TRIVIAL(debug) << "iterated something " << e.key().c_str();
            if (e.key() == "114") {
                assert(!found_114);
                found_114 = true;
                continue;
            }
            if (e.key() == "1919") {
                assert(!found_1919);
                found_1919 = true;
                continue;
            }
            /* no other key, should be unreachable */
            std::cerr << "unexpected entry: " << e.key().c_str() << std::endl;
            assert(false);
        }
        assert(found_114 && found_1919);
    }

    /* experiment with load factor */
    {
        const size_t CAPACITY = 1024000;
        const float FILL_RATE = .75;
        const size_t TESTSET_SIZE = FILL_RATE * CAPACITY;
        BOOST_LOG_TRIVIAL(info) << "experiment with"
            << " capacity " << CAPACITY
            << " fill rate " << FILL_RATE
            << ", test set size " << TESTSET_SIZE;

        auto arr = std::make_unique<entry[]>(CAPACITY);
        gestalt::HeadlessHashTable<entry> hht(arr.get(), CAPACITY);

        yp::dump_load(YCSB_BIN,
            {{"workload", workload_path.string()}, {"fieldcount", "1"},
             {"recordcount", std::to_string(TESTSET_SIZE)}},
            load_dump_path);
        yp::trace trace;
        yp::parse(load_dump_path, trace, /*with_value*/false);

        size_t total_inserted = 0;
        for (const auto &t : trace) {
            try {
                hht.insert({t.okey, {}});
                ++total_inserted;
            } catch (const std::bad_alloc &exc) {
                BOOST_LOG_TRIVIAL(warning) << "insert failed for key " << t.okey;
            }
        }
        BOOST_LOG_TRIVIAL(info) << "successfully inserted " << total_inserted
            << " (" << 100. * total_inserted / trace.size() << "%)";

        size_t cum_dist = 0;
        unsigned deadcenter = 0;
        for (const auto &e : hht) {
            const auto dist = std::abs(hht.access_distance(e.key()));
            if (!dist)
                deadcenter++;
            cum_dist += std::abs(dist);
        }
        BOOST_LOG_TRIVIAL(info) << "abs access distance avg: "
            << 1. * cum_dist / TESTSET_SIZE;
        BOOST_LOG_TRIVIAL(info) << 1. * deadcenter / TESTSET_SIZE * 100
            << "% of data are placed exactly at their hashed location";
    }

    return 0;
}
