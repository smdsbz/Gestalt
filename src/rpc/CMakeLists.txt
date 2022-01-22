find_program(PROTOC protoc)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin)


set(PROTO_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR})

foreach(SERVICE
    ClusterMap)

set(${SERVICE}_PROTO "${CMAKE_CURRENT_SOURCE_DIR}/${SERVICE}.proto")
set(${SERVICE}_PROTO_SRC "${PROTO_OUT_DIR}/${SERVICE}.pb.cc")
set(${SERVICE}_PROTO_HDR "${PROTO_OUT_DIR}/${SERVICE}.pb.h")
set(${SERVICE}_GRPC_SRC "${PROTO_OUT_DIR}/${SERVICE}.grpc.pb.cc")
set(${SERVICE}_GRPC_HDR "${PROTO_OUT_DIR}/${SERVICE}.grpc.pb.h")
add_custom_command(
    OUTPUT ${${SERVICE}_PROTO_SRC} ${${SERVICE}_PROTO_HDR}
        ${${SERVICE}_GRPC_SRC} ${${SERVICE}_GRPC_HDR}
    COMMAND ${PROTOC}
    ARGS --grpc_out ${PROTO_OUT_DIR}
        --cpp_out ${PROTO_OUT_DIR}
        -I ${CMAKE_CURRENT_SOURCE_DIR}
        --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
        ${${SERVICE}_PROTO}
    DEPENDS ${${SERVICE}_PROTO})

add_library(${SERVICE}_grpc_proto SHARED
    ${${SERVICE}_PROTO_SRC}
    ${${SERVICE}_PROTO_HDR}
    ${${SERVICE}_GRPC_SRC}
    ${${SERVICE}_GRPC_HDR})
target_include_directories(${SERVICE}_grpc_proto
    PUBLIC
        ${PROTO_OUT_DIR})
target_link_libraries(${SERVICE}_grpc_proto
    PUBLIC
        grpc++_reflection
        grpc++
        protobuf)

endforeach()
