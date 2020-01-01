LDFLAGS = -L/usr/local/lib `pkg-config --libs protobuf grpc++ plasma arrow hiredis`\
	  -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
	  -lgrpc -ldl -lpthread -lz

CXX = g++
CPPFLAGS += `pkg-config --cflags protobuf grpc plasma hiredis` -Iutil
CXXFLAGS += -std=c++11 -g

PROTOC = protoc
PROTOS_PATH = .
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

PROTO_OBJS = object_store.pb.o object_store.grpc.pb.o
UTILS_OBJS = util/logging.o util/socket_utils.o util/plasma_utils.o
OBJECT_STORE_OBJS = protocol.o notification.o global_control_store.o object_store_state.o \
	object_writer.o object_sender.o object_control.o distributed_object_store.o

all: multicast_test reduce_test

multicast_test: $(PROTO_OBJS) $(UTILS_OBJS) $(OBJECT_STORE_OBJS) multicast_test.o
	$(CXX) $^ $(LDFLAGS) -o $@

reduce_test: $(PROTO_OBJS) $(UTILS_OBJS) $(OBJECT_STORE_OBJS) reduce_test.o
	$(CXX) $^ $(LDFLAGS) -o $@

%.grpc.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

%.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=. $<

clean:
	rm -rf multicast_test reduce_test *.o *.pb.cc *.pb.h util/*.o
