set(TARGET lib${PROJECT_NAME}server)
add_library(${TARGET} STATIC server.cpp)
find_package(Boost REQUIRED COMPONENTS headers log)
target_link_libraries(${TARGET}
    INTERFACE
        Boost::headers
    PRIVATE
        Boost::log
        gestalt::misc::numa gestalt::misc::ddio)

set(TARGET ${PROJECT_NAME}_server)
add_executable(${TARGET} main.cpp)
find_package(Boost REQUIRED COMPONENTS program_options log)
target_link_libraries(${TARGET}
    PRIVATE
        Boost::program_options Boost::log
        lib${PROJECT_NAME}server)
