add_subdirectory(${CMAKE_SOURCE_DIR}/3rd_party/ycsb_parser ycsb_parser)
add_subdirectory(${CMAKE_SOURCE_DIR}/3rd_party/ycsb ycsb)

add_executable(hash-fill-factor main.cpp)
find_package(Boost REQUIRED COMPONENTS log)
target_link_libraries(hash-fill-factor
    PRIVATE
        Boost::log
        ycsb_parser ycsb
        gestalt::headless_hashtable isal)
