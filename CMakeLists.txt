cmake_minimum_required(VERSION 3.5)
project(chronostore)

enable_testing()

# include_directories(SYSTEM /usr/include/mpi)
include_directories(include)
include_directories(test)

find_package(HDF5 REQUIRED)

# Set the include directories
include_directories(${HDF5_INCLUDE_DIRS})
link_directories(/usr/lib/x86_64-linux-gnu/hdf5/serial)
link_directories(/usr/lib/x86_64-linux-gnu/hdf5/serial/include)
link_directories(/usr/include/mpi)

# set(CMAKE_C_COMPILER mpicc)
# set(CMAKE_CXX_COMPILER mpicxx)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${MPI_COMPILE_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${MPI_COMPILE_FLAGS}")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${MPI_LINK_FLAGS}")

# Set the build directory to a subdirectory called "build"
set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR}/build)

# Create the executable target
add_executable(chronostore main.cpp src/write.cpp src/event.cpp src/read.cpp src/chunkattr.cpp test/readQuery.cpp)

target_link_libraries(chronostore ${HDF5_LIBRARIES})




# cmake_minimum_required(VERSION 3.5)
# project(chronostore)

# enable_testing()
# find_package(HDF5 REQUIRED)

# # Set the include directories
# include_directories(${HDF5_INCLUDE_DIRS})
# include_directories(SYSTEM /usr/include/mpi)
# include_directories(include)
# include_directories(test)

# link_directories(/usr/lib/x86_64-linux-gnu/hdf5/serial)
# link_directories(/usr/lib/x86_64-linux-gnu/hdf5/serial/include)
# link_directories(/usr/include/mpi)

# set(CMAKE_C_COMPILER mpicc)
# set(CMAKE_CXX_COMPILER mpicxx)
# set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${MPI_COMPILE_FLAGS}")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${MPI_COMPILE_FLAGS}")
# set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${MPI_LINK_FLAGS}")

# # Set the build directory to a subdirectory called "build"
# set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR}/build)

# # Create the executable target
# add_executable(chronostore main.cpp src/write.cpp src/event.cpp src/read.cpp src/chunkattr.cpp test/readQuery.cpp)

# target_link_libraries(chronostore hdf5 ${HDF5_LIBRARIES})
