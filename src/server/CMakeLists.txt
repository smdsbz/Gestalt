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
# NOTE: add headers as well, otherwise we don't get IntelliSense :)
add_library(${TARGET} server.cpp server.hpp
    session_servicer.cpp session_servicer.hpp)
add_library(gestalt::lib::server ALIAS ${TARGET})
find_package(Boost REQUIRED COMPONENTS headers log system)
target_link_libraries(${TARGET}
    PUBLIC
        Boost::headers
    PRIVATE
        Boost::log Boost::system
        gestalt::misc::numa gestalt::misc::ddio
        rdmacm ibverbs
        pmem
        gestalt::headless_hashtable
        ClusterMap_grpc_proto Session_grpc_proto)

set(TARGET gestalt_server)
add_executable(${TARGET} main.cpp)
set_target_properties(${TARGET}
    PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/)
find_package(Boost REQUIRED COMPONENTS program_options log)
target_link_libraries(${TARGET}
    Boost::program_options Boost::log
    gestalt::lib::server)
