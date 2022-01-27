include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# Utility helpers
add_subdirectory(misc)

# RPC definitions
add_subdirectory(rpc)

add_subdirectory(monitor)
add_subdirectory(server)
add_subdirectory(client)
