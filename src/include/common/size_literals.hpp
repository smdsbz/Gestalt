/**
 * @file size_literals.hpp
 * Handy C++ size / volume literals
 */

#pragma once

#include <cstdint>
#include <string>


namespace {
constexpr size_t operator "" _B(unsigned long long int s) { return s; }
constexpr size_t operator "" _K(unsigned long long int s) { return s * 1024_B; }
constexpr size_t operator "" _M(unsigned long long int s) { return s * 1024_K; }
constexpr size_t operator "" _G(unsigned long long int s) { return s * 1024_M; }

std::string to_human_readable(size_t i)
{
    if (i > 1_G)
        return std::to_string((double)i / 1_G) + "G";
    if (i > 1_M)
        return std::to_string((double)i / 1_M) + "M";
    if (i > 1_K)
        return std::to_string((double)i / 1_K) + "K";
    return std::to_string(i) + "B";
}
}   /* anonymous namespace */
