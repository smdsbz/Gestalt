set(TARGET gestalt_headless_hashtable)
add_library(${TARGET} INTERFACE)
add_library(gestalt::headless_hashtable ALIAS ${TARGET})
target_include_directories(${TARGET}
    INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR})
find_package(Boost REQUIRED COMPONENTS headers)
link_libraries(Boost::headers)
target_link_libraries(${TARGET}
    INTERFACE
        Boost::headers)

# server object as library
set(TARGET gestaltserver)
add_library(${TARGET} SHARED server.cpp server.hpp)
add_library(gestalt::shared::server ALIAS ${TARGET})
find_package(Boost REQUIRED COMPONENTS headers log)
target_link_libraries(${TARGET}
    PUBLIC
        Boost::headers
    PRIVATE
        Boost::log
        gestalt::misc::numa gestalt::misc::ddio
        rdmacm ibverbs
        gestalt::headless_hashtable
        ClusterMap_grpc_proto)

set(TARGET gestalt_server)
add_executable(${TARGET} main.cpp)
set_target_properties(${TARGET}
    PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/)
find_package(Boost REQUIRED COMPONENTS program_options log)
target_link_libraries(${TARGET}
    Boost::program_options Boost::log
    gestalt::shared::server)
