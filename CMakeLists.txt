cmake_minimum_required(VERSION 3.14)
project(Hoplite VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O2")

#if (MSVC)
#    # warning level 4 and all warnings as errors
#    add_compile_options(/W4)
#else()
#    # lots of warnings and all warnings as errors
#    add_compile_options(-Wall -Wextra -pedantic)
#endif()

# Cmake find modules
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(MPI REQUIRED)
message(STATUS "Using Protobuf ${Protobuf_VERSION}, gRPC ${gRPC_VERSION}, MPI ${MPI_VERSION}")

include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${gRPC_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_CURRENT_LIST_DIR}/src/)

protobuf_generate_cpp(HOPLITE_PROTO_SRCS HOPLITE_PROTO_HDRS src/protocol/object_store.proto)
protobuf_generate_python(HOPLITE_PROTO_PY src/protocol/object_store.proto)
grpc_generate_cpp(HOPLITE_GRPC_SRCS HOPLITE_GRPC_HDRS "${CMAKE_CURRENT_BINARY_DIR}" src/protocol/object_store.proto)

file(GLOB hoplite_common_SRC "src/common/*.h" "src/common/*.cc")
file(GLOB hoplite_utils_SRC "src/util/*.h" "src/util/*.cc")
add_library(hoplite_common ${hoplite_common_SRC})
add_library(hoplite_utils ${hoplite_utils_SRC})
file(GLOB hoplite_client_SRC "src/client/*.h" "src/client/*.cc")

add_library(hoplite_client ${hoplite_client_SRC}
        ${HOPLITE_PROTO_SRCS} ${HOPLITE_PROTO_HDRS}
        ${HOPLITE_GRPC_SRCS} ${HOPLITE_GRPC_HDRS})

add_library(hoplite_client_lib SHARED ${hoplite_client_SRC}
        ${HOPLITE_PROTO_SRCS} ${HOPLITE_PROTO_HDRS}
        ${HOPLITE_GRPC_SRCS} ${HOPLITE_GRPC_HDRS})
set_target_properties(hoplite_client_lib PROPERTIES
        PUBLIC_HEADER src/client/distributed_object_store.h
        POSITION_INDEPENDENT_CODE ON)
target_include_directories(hoplite_client_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src/client)
target_link_libraries(hoplite_client_lib PUBLIC hoplite_utils hoplite_common
        ${Protobuf_LIBRARIES}
        ${gRPC_LIBRARIES}
        gRPC::grpc++_reflection
        protobuf::libprotobuf
        PRIVATE Threads::Threads
        ${CMAKE_DL_LIBS})

# build executables

# object directory
file(GLOB object_directory_SRC "src/object_directory/*.h" "src/object_directory/*.cc")
add_executable(notification
        ${object_directory_SRC}
        ${HOPLITE_PROTO_SRCS} ${HOPLITE_PROTO_HDRS}
        ${HOPLITE_GRPC_SRCS} ${HOPLITE_GRPC_HDRS})
target_include_directories(notification PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src/object_directory)
target_link_libraries(notification PUBLIC hoplite_utils hoplite_common
        PRIVATE Threads::Threads
        ${CMAKE_DL_LIBS}
        ${Protobuf_LIBRARIES}
        ${gRPC_LIBRARIES}
        gRPC::grpc++_reflection
        protobuf::libprotobuf)

# tests
# TODO: notification_server_test
set(hoplite_communication_tests multicast_test reduce_test subset_reduce_test allreduce_test gather_test allgather_test)
foreach (testname ${hoplite_communication_tests})
    add_executable(${testname} "src/tests/${testname}.cc")
    set_target_properties(${testname}
            PROPERTIES
            ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/tests/"
            LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/tests/"
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/tests/")
    target_include_directories(${testname} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src/client)
    target_link_libraries(${testname} PUBLIC hoplite_common hoplite_utils hoplite_client
            PRIVATE Threads::Threads
            ${CMAKE_DL_LIBS}
            ${Protobuf_LIBRARIES}
            ${gRPC_LIBRARIES}
            ${MPI_LIBRARIES}
            gRPC::grpc++_reflection
            protobuf::libprotobuf)
endforeach (testname ${hoplite_communication_tests})

add_executable(reduce_dependency_test "src/tests/reduce_dependency_test.cc" "src/object_directory/reduce_dependency.cc")
target_link_libraries(reduce_dependency_test PUBLIC hoplite_common hoplite_utils
        PRIVATE Threads::Threads
        ${CMAKE_DL_LIBS}
        ${Protobuf_LIBRARIES}
        ${gRPC_LIBRARIES}
        gRPC::grpc++_reflection
        protobuf::libprotobuf)
set_target_properties(reduce_dependency_test
        PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/tests/"
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/tests/"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/tests/")

# install(TARGETS hoplite_client_lib
#    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
#    PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

# TODO: unconverted makefile content:
# python/object_store_pb2_grpc.py:
#	python -m pip install grpcio-tools
#	python -m grpc_tools.protoc -Isrc --python_out=python --grpc_python_out=python src/object_store.proto
#
#notification_server_test: $(PROTO_OBJS) $(UTILS_OBJS) $(COMMON_OBJS) src/notification_server_test.o
#	$(CXX) $^ $(LDFLAGS) -o $@
#
#py_distributed_object_store: libdistributed_object_store.so
#	python setup.py build_ext --inplace && cp *distributed_object_store*.so python/hoplite && cp notification python/hoplite