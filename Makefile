# EFA libfabric (from AWS EFA installer at /opt/amazon/efa)
EFA_PREFIX ?= /opt/amazon/efa

CXX      = g++
CXXFLAGS = -std=c++17 -O2 -g -Wall \
           -I$(EFA_PREFIX)/include
LDFLAGS  = -L$(EFA_PREFIX)/lib64 -Wl,-rpath,$(EFA_PREFIX)/lib64
LDLIBS   = -lfabric -lpthread -lisal

BINARIES = build/server build/client

.PHONY: all clean

all: build $(BINARIES)

build:
	mkdir -p build

build/server: src/server.cpp src/common.hpp src/protocol.hpp
	$(CXX) $(CXXFLAGS) -o $@ src/server.cpp $(LDFLAGS) $(LDLIBS)

build/client: src/client.cpp src/common.hpp src/protocol.hpp src/ec.hpp
	$(CXX) $(CXXFLAGS) -o $@ src/client.cpp $(LDFLAGS) $(LDLIBS)

clean:
	rm -rf build
