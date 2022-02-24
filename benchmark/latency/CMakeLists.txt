add_executable(benchmark_latency main.cpp)
set_target_properties(benchmark_latency
    PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/)
find_package(Boost REQUIRED COMPONENTS log program_options)
target_link_libraries(benchmark_latency
    Boost::log Boost::program_options
    ycsb_parser ycsb
    gestalt::lib::client)
