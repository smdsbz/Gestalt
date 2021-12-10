project(ycsb)

# Fetch YCSB executable
include(FetchContent)
FetchContent_Declare(
    ycsb_release
    URL     https://hub.fastgit.org/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz)
FetchContent_MakeAvailable(ycsb_release)

# Register YCSB path
configure_file(ycsb.h.in ${PROJECT_BINARY_DIR}/ycsb.h)
add_library(ycsb INTERFACE)
target_include_directories(ycsb
    INTERFACE
        ${PROJECT_BINARY_DIR})
