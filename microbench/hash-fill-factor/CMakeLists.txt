project(hash-fill-factor)

add_subdirectory(${CMAKE_SOURCE_DIR}/3rd_party/ycsb_parser ycsb_parser)
add_subdirectory(${CMAKE_SOURCE_DIR}/3rd_party/ycsb ycsb)

add_executable(hash-fill-factor main.cpp)
target_link_libraries(hash-fill-factor
    PRIVATE
        ycsb_parser ycsb)
