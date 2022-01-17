# NUMA
set(TARGET gestalt_misc_numa)
add_library(${TARGET} STATIC numa.cpp)
add_library(gestalt::misc::numa ALIAS ${TARGET})
target_include_directories(${TARGET}
    PUBLIC
        ${CMAKE_SOURCE_DIR}/src/include)
find_package(Boost REQUIRED COMPONENTS headers) # property_tree
# target_compile_definitions(${TARGET}
#     PRIVATE GET_NUMA_WITH_LIBNDCTL)
target_link_libraries(${TARGET}
    PRIVATE
        Boost::headers
        # ndctl
        rdmacm)

# DDIO
set(TARGET gestalt_misc_ddio)
add_library(${TARGET} STATIC ddio.cpp)
add_library(gestalt::misc::ddio ALIAS ${TARGET})
target_include_directories(${TARGET}
    PUBLIC
        ${CMAKE_SOURCE_DIR}/src/include)
find_package(Boost REQUIRED COMPONENTS headers)
target_link_libraries(${TARGET}
    PRIVATE
        Boost::headers)
