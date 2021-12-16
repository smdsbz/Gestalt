# Headless HashTable
set(TARGET gestalt_headless_hashtable)
add_library(${TARGET} INTERFACE)
add_library(gestalt::headless_hashtable ALIAS ${TARGET})
target_include_directories(${TARGET}
    INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR})
find_package(Boost REQUIRED COMPONENTS headers)
target_link_libraries(${TARGET}
    INTERFACE
        Boost::headers)
