find_package(Boost REQUIRED COMPONENTS unit_test_framework log)
link_libraries(Boost::unit_test_framework Boost::log)

include_directories(${CMAKE_SOURCE_DIR}/src/include)

# Misc
add_executable(test_misc misc.cpp)
target_link_libraries(test_misc
    PRIVATE
        gestalt::misc::numa pmem
        gestalt::misc::ddio)


add_test(unittest_all
    test_misc)
