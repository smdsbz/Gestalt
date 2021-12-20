project(playground_rdma-hello-world)

add_executable(${PROJECT_NAME} main.cpp)

find_package(Boost REQUIRED COMPONENTS unit_test_framework)
target_link_libraries(${PROJECT_NAME}
    Boost::unit_test_framework ibverbs rdmacm pthread)
