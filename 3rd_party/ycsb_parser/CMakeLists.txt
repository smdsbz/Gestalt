cmake_minimum_required(VERSION 3.16)

project(ycsb_parser)
set(CMAKE_CXX_STANDARD 20)

find_package(Boost REQUIRED COMPONENTS log)

add_library(ycsb_parser STATIC ycsb_parser.cpp)
target_include_directories(ycsb_parser
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(ycsb_parser
    PRIVATE
        Boost::log)
