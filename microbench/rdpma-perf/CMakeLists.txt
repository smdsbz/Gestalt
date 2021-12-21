project(microbench_rdpma-perf)

find_package(Boost REQUIRED COMPONENTS program_options)

add_executable(${PROJECT_NAME}-server server.cpp)
target_link_libraries(${PROJECT_NAME}-server
    PRIVATE
        Boost::program_options
        pmem rdmacm ibverbs
        gestalt::misc::numa gestalt::misc::ddio)

add_executable(${PROJECT_NAME}-client client.cpp)
target_include_directories(${PROJECT_NAME}-client
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include)
target_link_libraries(${PROJECT_NAME}-client
    PRIVATE
        Boost::program_options
        rdmacm ibverbs)
