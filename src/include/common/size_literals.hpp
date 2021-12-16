/**
 * @file size_literals.hpp
 * Handy C++ size / volume literals
 */

#pragma once

#include <cstdint>


constexpr size_t operator "" _B(unsigned long long int s) { return s; }
constexpr size_t operator "" _K(unsigned long long int s) { return s * 1024_B; }
constexpr size_t operator "" _M(unsigned long long int s) { return s * 1024_K; }
constexpr size_t operator "" _G(unsigned long long int s) { return s * 1024_M; }
