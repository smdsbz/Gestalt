cmake_minimum_required(VERSION 3.16)

project(gestalt CXX)
set(CMAKE_CXX_STANDARD 20)

# libibverbs
add_compile_options(
    -Wno-deprecated-enum-enum-conversion
    -Wno-unused-result)

add_subdirectory(src)

add_subdirectory(${CMAKE_SOURCE_DIR}/3rd_party/ycsb_parser ycsb_parser)
add_subdirectory(${CMAKE_SOURCE_DIR}/3rd_party/ycsb ycsb)

add_subdirectory(benchmark)

add_subdirectory(playground EXCLUDE_FROM_ALL)
add_subdirectory(microbench EXCLUDE_FROM_ALL)

enable_testing()
add_subdirectory(unittest EXCLUDE_FROM_ALL)
