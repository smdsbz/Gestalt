project(playground_librdmacm-examples)

add_executable(${PROJECT_NAME}-server rdma_server.c)
target_link_libraries(${PROJECT_NAME}-server
    ibverbs rdmacm)

add_executable(${PROJECT_NAME}-client rdma_client.c)
target_link_libraries(${PROJECT_NAME}-client
    ibverbs rdmacm)
