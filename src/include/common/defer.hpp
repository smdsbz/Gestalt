/**
 * @file defer.hpp
 * Golang-style `defer`
 */

#pragma once

#include <functional>

#define GESTALT_CONCAT(a, b) a ## b
#define __gestalt_defer_impl(ptr, dfn, l)                       \
    std::unique_ptr<std::remove_pointer<decltype(ptr)>::type,   \
                    std::function<void(decltype(ptr))>>         \
    GESTALT_CONCAT(__dtor, l) (ptr, [&](decltype(ptr) p) { dfn(p); })
/**
 * Golang-style defer
 * @param ptr resource handle as pointer
 * @param dfn destructor of type `std::function<void(decltype(ptr))>`
 */
#define defer(ptr, dfn) __gestalt_defer_impl(ptr, dfn, __LINE__)
