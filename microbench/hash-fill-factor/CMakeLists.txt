project(microbench_hash-fill-factor)

add_executable(${PROJECT_NAME} main.cpp)
find_package(Boost REQUIRED COMPONENTS log)
target_include_directories(${PROJECT_NAME}
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include/)
target_link_libraries(${PROJECT_NAME}
    Boost::log
    gestalt::headless_hashtable
    ycsb_parser ycsb
    isal)
