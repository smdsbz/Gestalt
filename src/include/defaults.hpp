/**
 * @file
 */

#pragma once

#include <filesystem>
#include <vector>


namespace gestalt {
namespace defaults {

using namespace std;

/** possible config file paths, in precedence descending order */
const vector<filesystem::path> config_paths{
    "/etc/gestalt/gestalt.conf",
    "gestalt.conf",
    "etc/gestalt/gestalt.conf",
    "../../etc/gestalt/gestalt.conf",
};

/**
 * maximum entries in a locator cache
 *
 * A cache entry includes a 496B key and optional locators, which are just a
 * small vector of packed three numbers, therefore a cache of maximum 10 million
 * entries will cost us around 2 * 1e7 * 512B ~= 10MB memory, when populated.
 */
constexpr size_t client_locator_cache_size = 1e7;
constexpr size_t client_redirection_cache_size = client_locator_cache_size * .1;

}   /* namespace defaults */
}   /* namespace gestalt */
