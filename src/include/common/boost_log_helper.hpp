/**
 * @file boost_log_helper.hpp
 *
 * Handy macros
 */

#pragma once

#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>


#define __set_log_level(lv)                                                 \
do {                                                                        \
    namespace logging = boost::log;                                         \
    logging::core::get()->set_filter(                                       \
        logging::trivial::severity >= logging::trivial::lv                  \
    );                                                                      \
} while (0)
#define __match_log_level(cmp, lv, isset)                                   \
do {                                                                        \
    namespace logging = boost::log;                                         \
    if (!isset && cmp == #lv) {                                             \
        __set_log_level(lv);                                                \
        isset = true;                                                       \
    }                                                                       \
} while (0)
/**
 * Set minimum Boost::log severity, if no match, defaults to "info"
 * @param lv wanted logging level
 */
#define set_boost_log_level(lv)                                             \
do {                                                                        \
    namespace logging = boost::log;                                         \
    bool isset = false;                                                     \
    __match_log_level(lv, trace, isset);                                    \
    __match_log_level(lv, debug, isset);                                    \
    __match_log_level(lv, info, isset);                                     \
    __match_log_level(lv, warning, isset);                                  \
    __match_log_level(lv, fatal, isset);                                    \
    if (!isset)                                                             \
        __set_log_level(info);                                              \
} while (0)
