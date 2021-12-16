find_package(Boost REQUIRED COMPONENTS unit_test_framework log)
link_libraries(Boost::unit_test_framework Boost::log)

include_directories(src/include)

enable_testing()

# Misc - NUMA
add_executable(numa numa.cpp)
target_link_libraries(numa
    PRIVATE
        gestalt::misc::numa pmem)
