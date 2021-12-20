cmake_minimum_required(VERSION 3.16)

project(gestalt)
set(CMAKE_CXX_STANDARD 20)

# libibverbs
add_compile_options(-Wno-deprecated-enum-enum-conversion)

add_subdirectory(playground)

add_subdirectory(src)
add_subdirectory(microbench)

enable_testing()
add_subdirectory(unittest)
