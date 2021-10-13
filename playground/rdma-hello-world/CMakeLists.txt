project(rdma-hello-world)

find_package(Boost REQUIRED COMPONENTS unit_test_framework)

add_executable(${PROJECT_NAME} main.cpp)
target_link_libraries(${PROJECT_NAME}
    Boost::unit_test_framework
    ibverbs)
target_compile_definitions(${PROJECT_NAME}
    PRIVATE
        BOOST_TEST_DYN_LINK)
