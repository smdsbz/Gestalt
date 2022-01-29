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

constexpr size_t client_redirection_cache_size = 1e4;

}   /* namespace defaults */
}   /* namespace gestalt */
