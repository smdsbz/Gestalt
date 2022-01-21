set(TARGET gestalt_monitor)
add_executable(${TARGET} main.cpp)
set_target_properties(${TARGET}
    PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/)
find_package(Boost REQUIRED COMPONENTS headers log program_options)
target_link_libraries(${TARGET}
    Boost::headers
    Boost::log Boost::program_options
    ClusterMap_grpc_proto)
