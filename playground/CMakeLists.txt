project(playground)

include_directories(${PROJECT_BINARY_DIR})
set(playground_rnic_ip "10.0.2.15")
set(playground_rnic_port "11451")
configure_file(include/playground/playground.h.in ${PROJECT_BINARY_DIR}/playground/playground.h)

find_package(Boost REQUIRED)
add_compile_definitions(BOOST_TEST_DYN_LINK)

add_subdirectory(rdma-hello-world)
add_subdirectory(librdmacm-examples)
