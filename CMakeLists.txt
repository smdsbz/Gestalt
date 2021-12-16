cmake_minimum_required(VERSION 3.16)

project(gestalt)
set(CMAKE_CXX_STANDARD 20)

# libibverbs
add_compile_options(-Wno-deprecated-enum-enum-conversion)

add_subdirectory(playground EXCLUDE_FROM_ALL)
add_subdirectory(microbench EXCLUDE_FROM_ALL)

add_subdirectory(unittest)
add_subdirectory(src)
