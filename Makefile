LDFLAGS = -L/usr/local/lib `pkg-config --libs protobuf grpc++ plasma arrow hiredis`\
	  -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
	  -lgrpc -ldl -lpthread -lz

CXX = g++
CPPFLAGS += `pkg-config --cflags protobuf grpc plasma hiredis` -Iutil
CXXFLAGS += -std=c++11 -g

PROTOC = /usr/local/bin/protoc
PROTOS_PATH = .
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

object_store: object_store.pb.o object_store.grpc.pb.o util/logging.o util/socket_utils.o \
              util/plasma_utils.o protocol.o global_control_store.o object_store_state.o \
			  object_writer.o object_control.o distributed_object_store.o object_store.o
	$(CXX) $^ $(LDFLAGS) -o $@

%.grpc.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

%.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=. $<

clean:
	rm -rf object_store *.o *.pb.cc *.pb.h util/*.o
