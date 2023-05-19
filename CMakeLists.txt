cmake_minimum_required(VERSION 3.5)
project(chronostore)

include_directories(include)
include_directories(test)

find_package(HDF5 REQUIRED)

# Set the include directories
include_directories(${HDF5_INCLUDE_DIRS})
link_directories(/usr/lib/x86_64-linux-gnu/hdf5/serial)
link_directories(/usr/lib/x86_64-linux-gnu/hdf5/serial/include)

# Set the build directory to a subdirectory called "build"
set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR}/build)

# Create the executable target
add_executable(chronostore main.cpp src/storyWriter.cpp src/storyReader.cpp test/testReadWrite.cpp)

target_link_libraries(chronostore ${HDF5_LIBRARIES})
