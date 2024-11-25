HOST_SYSTEM = $(shell uname | cut -f 1 -d_)
SYSTEM ?= $(HOST_SYSTEM)
CXX ?= g++
CPPFLAGS += `pkg-config --cflags protobuf grpc`
CXXFLAGS += -std=c++17 -Wall -Wextra -Werror

ifeq ($(SYSTEM),Darwin)
LDFLAGS += -L/usr/local/lib `pkg-config --libs --static protobuf grpc++`\
           -pthread\
           -lgrpc++_reflection\
           -ldl -lglog -lrabbitmq -ljsoncpp
else
LDFLAGS += -L/usr/local/lib `pkg-config --libs --static protobuf grpc++`\
           $(PROTOBUF_UTF8_RANGE_LINK_LIBS) \
           -pthread\
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
           -ldl -lglog -lrabbitmq -ljsoncpp
endif

PROTOC = protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`
PROTOS_PATH = .


all: tsc tsd coordinator synchronizer 

tsc: client.o coordinator.pb.o coordinator.grpc.pb.o sns.pb.o sns.grpc.pb.o tsc.o
	$(CXX) $^ $(LDFLAGS) -g -o $@

tsd: coordinator.pb.o coordinator.grpc.pb.o sns.pb.o sns.grpc.pb.o tsd.o sns_db.o
	$(CXX) $^ $(LDFLAGS) -g -o $@

coordinator: coordinator.pb.o coordinator.grpc.pb.o coordinator.o
	$(CXX) $^ $(LDFLAGS) -g -o $@

synchronizer: coordinator.pb.o coordinator.grpc.pb.o sns.pb.o sns.grpc.pb.o synchronizer.o sns_db.o
	$(CXX) $^ $(LDFLAGS) -g -o $@


.PRECIOUS: %.grpc.pb.cc
%.grpc.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

.PRECIOUS: %.pb.cc
%.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=. $<

clean:
	rm -f *.txt *.o *.pb.cc *.pb.h tsc tsd coordinator synchronizer
