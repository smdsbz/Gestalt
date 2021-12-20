find_package(Boost REQUIRED COMPONENTS unit_test_framework log)
link_libraries(Boost::unit_test_framework Boost::log)

include_directories(src/include)

enable_testing()

# Misc
add_executable(test_misc misc.cpp)
target_link_libraries(test_misc
    PRIVATE
        gestalt::misc::numa pmem
        gestalt::misc::ddio)
