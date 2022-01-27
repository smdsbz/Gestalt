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
        Boost::log Boost::system
        rdmacm ibverbs
        ClusterMap_grpc_proto Session_grpc_proto)
