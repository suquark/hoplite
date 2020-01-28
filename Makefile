LDFLAGS = -L/usr/local/lib `pkg-config --libs protobuf grpc++ plasma arrow hiredis`\
	  -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
	  -lgrpc -ldl -lpthread -lz

CXX = g++
CPPFLAGS += `pkg-config --cflags protobuf grpc plasma hiredis` -Isrc/util -Isrc
CXXFLAGS += -std=c++11 -O2 -g -fPIC

PROTOC = protoc
PROTOS_PATH = src/
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

PROTO_OBJS = src/object_store.pb.o src/object_store.grpc.pb.o
UTILS_OBJS = src/util/logging.o src/util/socket_utils.o
COMMON_OBJS = src/common/id.o src/common/buffer.o src/common/status.o 
OBJECT_STORE_OBJS = src/local_store_client.o src/global_control_store.o src/object_store_state.o \
	src/object_writer.o src/object_sender.o src/distributed_object_store.o

all: notification multicast_test reduce_test allreduce_test gather_test allgather_test py_distributed_object_store python/object_store_pb2_grpc.py

python/object_store_pb2_grpc.py:
	python -m pip install grpcio-tools
	python -m grpc_tools.protoc -Isrc --python_out=python --grpc_python_out=python src/object_store.proto

notification: $(PROTO_OBJS) $(UTILS_OBJS) $(COMMON_OBJS) src/notification.o
	$(CXX) $^ $(LDFLAGS) -o $@

notification_server_test: $(PROTO_OBJS) $(UTILS_OBJS) $(COMMON_OBJS) src/notification_server_test.o
	$(CXX) $^ $(LDFLAGS) -o $@

py_distributed_object_store: libdistributed_object_store.so
	python setup.py build_ext --inplace && cp *cpython*.so python/

libdistributed_object_store.so: $(PROTO_OBJS) $(UTILS_OBJS) $(COMMON_OBJS) $(OBJECT_STORE_OBJS)
	$(CXX) $^ $(LDFLAGS) -shared -o $@

multicast_test: $(PROTO_OBJS) $(UTILS_OBJS) $(COMMON_OBJS) $(OBJECT_STORE_OBJS) multicast_test.o
	$(CXX) $^ $(LDFLAGS) -o $@

reduce_test: $(PROTO_OBJS) $(UTILS_OBJS) $(COMMON_OBJS) $(OBJECT_STORE_OBJS) reduce_test.o
	$(CXX) $^ $(LDFLAGS) -o $@

allreduce_test: $(PROTO_OBJS) $(UTILS_OBJS) $(COMMON_OBJS) $(OBJECT_STORE_OBJS) allreduce_test.o
	$(CXX) $^ $(LDFLAGS) -o $@

gather_test: $(PROTO_OBJS) $(UTILS_OBJS) $(COMMON_OBJS) $(OBJECT_STORE_OBJS) gather_test.o
	$(CXX) $^ $(LDFLAGS) -o $@

allgather_test: $(PROTO_OBJS) $(UTILS_OBJS) $(COMMON_OBJS) $(OBJECT_STORE_OBJS) allgather_test.o
	$(CXX) $^ $(LDFLAGS) -o $@

%.grpc.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --grpc_out=src/ --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

%.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=src/ $<

clean_bins:
	rm -rf multicast_test reduce_test all_reduce_test python/*.cpp python/*.so *.so
clean:
	rm -rf notification notification_server_test multicast_test reduce_test all_reduce_test src/*.o src/*.pb.cc src/*.pb.h src/util/*.o python/*.cpp python/*.so python/object_store_pb2_grpc.py python/object_store_pb2.py *.o *.so
