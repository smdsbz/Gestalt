include_directories(${CMAKE_CURRENT_SOURCE_DIR})
find_package(Boost REQUIRED COMPONENTS headers)
link_libraries(Boost::headers)
