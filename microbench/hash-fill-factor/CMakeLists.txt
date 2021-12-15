project(hash-fill-factor)

add_subdirectory(${CMAKE_SOURCE_DIR}/3rd_party/ycsb_parser ycsb_parser)
add_subdirectory(${CMAKE_SOURCE_DIR}/3rd_party/ycsb ycsb)

find_package(Boost REQUIRED COMPONENTS headers log)
link_libraries(Boost::headers Boost::log)

add_executable(hash-fill-factor main.cpp)
target_include_directories(hash-fill-factor
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include)
target_link_libraries(hash-fill-factor
    PRIVATE
        ycsb_parser ycsb)
