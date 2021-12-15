project(source)

find_package(Boost REQUIRED COMPONENTS headers)

include_directories(include)
link_libraries(Boost::headers)
