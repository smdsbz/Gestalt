/**
 * @file defer.hpp
 * Golang-style `defer`
 */

#pragma once

#include <memory>
#include <functional>

#define GESTALT_CONCAT(a, b) a ## b
#define __gestalt_defer_impl(dfn, l)                            \
    std::unique_ptr<void, std::function<void(void*)>>           \
    GESTALT_CONCAT(__dtor, l) ((void*)1, [&](void*) { dfn(); })
/**
 * Golang-style defer
 * @param dfn destructor function, it takes no argument, and return value is
 *      ignored
 */
#define defer(dfn) __gestalt_defer_impl(dfn, __LINE__)
