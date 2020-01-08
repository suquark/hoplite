LDFLAGS = -L/usr/local/lib `pkg-config --libs protobuf grpc++ plasma arrow hiredis`\
	  -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
	  -lgrpc -ldl -lpthread -lz

CXX = g++
CPPFLAGS += `pkg-config --cflags protobuf grpc plasma hiredis` -Isrc/util -Isrc
CXXFLAGS += -std=c++11 -O2 -g

PROTOC = protoc
PROTOS_PATH = src/
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

PROTO_OBJS = src/object_store.pb.o src/object_store.grpc.pb.o
UTILS_OBJS = src/util/logging.o src/util/socket_utils.o
OBJECT_STORE_OBJS = src/local_store_client src/global_control_store.o src/object_store_state.o \
	src/object_writer.o src/object_sender.o src/object_control.o src/distributed_object_store.o

all: notification multicast_test reduce_test allreduce_test

notification: $(PROTO_OBJS) $(UTILS_OBJS) src/notification.o
	$(CXX) $^ $(LDFLAGS) -o $@

multicast_test: $(PROTO_OBJS) $(UTILS_OBJS) $(OBJECT_STORE_OBJS) multicast_test.o
	$(CXX) $^ $(LDFLAGS) -o $@

reduce_test: $(PROTO_OBJS) $(UTILS_OBJS) $(OBJECT_STORE_OBJS) reduce_test.o
	$(CXX) $^ $(LDFLAGS) -o $@

allreduce_test: $(PROTO_OBJS) $(UTILS_OBJS) $(OBJECT_STORE_OBJS) allreduce_test.o
	$(CXX) $^ $(LDFLAGS) -o $@

%.grpc.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --grpc_out=src/ --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

%.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=src/ $<

clean:
	rm -rf multicast_test reduce_test all_reduce_test src/*.o src/*.pb.cc src/*.pb.h src/util/*.o *.o
