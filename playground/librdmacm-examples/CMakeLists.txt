project(librdmacm-examples)

add_executable(rdma-server rdma_server.c)
target_link_libraries(rdma-server
    ibverbs rdmacm)

add_executable(rdma-client rdma_client.c)
target_link_libraries(rdma-client
    ibverbs rdmacm)
