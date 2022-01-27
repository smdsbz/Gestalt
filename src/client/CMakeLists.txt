set(TARGET gestaltclient)
add_library(${TARGET} client.cpp
    data_mapper.cpp
    rdma_connection_pool.cpp)
add_library(gestalt::lib::client ALIAS ${TARGET})
find_package(Boost REQUIRED COMPONENTS headers log system)
target_link_libraries(${TARGET}
    PUBLIC
        Boost::headers
    PRIVATE
        isal
        Boost::log Boost::system
        rdmacm ibverbs
        ClusterMap_grpc_proto Session_grpc_proto)

add_executable(dev_client dev_client.cpp)
set_target_properties(dev_client
    PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/)
find_package(Boost REQUIRED COMPONENTS log program_options)
target_link_libraries(dev_client
    Boost::log Boost::program_options
    gestalt::lib::client)
