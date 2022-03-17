add_executable(benchmark_dist_starter starter.cpp)
set_target_properties(benchmark_dist_starter
    PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/)
find_package(Boost REQUIRED COMPONENTS log program_options)
target_link_libraries(benchmark_dist_starter
    Boost::log Boost::program_options
    ycsb_parser ycsb
    gestalt::lib::client)

add_executable(benchmark_dist_follower follower.cpp)
set_target_properties(benchmark_dist_follower
    PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/)
find_package(Boost REQUIRED COMPONENTS log program_options)
target_link_libraries(benchmark_dist_follower
    Boost::log Boost::program_options
    ycsb_parser ycsb
    gestalt::lib::client)
