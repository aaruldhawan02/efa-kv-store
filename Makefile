# Use system libfabric (verbs/RoCE).
# Override FABRIC_PREFIX if libfabric is installed elsewhere, e.g.:
#   make FABRIC_PREFIX=/opt/amazon/efa   (EFA)
#   make FABRIC_PREFIX=/usr/local         (custom build)
FABRIC_PREFIX ?= /usr

CXX      = g++
CXXFLAGS = -std=c++17 -O2 -g -Wall \
           -I$(FABRIC_PREFIX)/include
LDFLAGS  = -L$(FABRIC_PREFIX)/lib -Wl,-rpath,$(FABRIC_PREFIX)/lib
LDLIBS   = -lfabric -lpthread -lisal

PYBIND_INC    := $(shell python3 -c "import pybind11; print(pybind11.get_include())" 2>/dev/null)
PYTHON_INC    := $(shell python3-config --includes 2>/dev/null)
PYTHON_SUFFIX := $(shell python3-config --extension-suffix 2>/dev/null)
PYMOD         := build/rdmastorage$(PYTHON_SUFFIX)

BINARIES = build/server build/client build/coordinator

.PHONY: all clean pymod

all: build $(BINARIES)

pymod: build $(PYMOD)

build:
	mkdir -p build

build/server: src/server.cpp src/common.hpp src/protocol.hpp
	$(CXX) $(CXXFLAGS) -o $@ src/server.cpp $(LDFLAGS) $(LDLIBS)

build/client: src/client.cpp src/common.hpp src/protocol.hpp src/ec.hpp src/client_lib.hpp
	$(CXX) $(CXXFLAGS) -o $@ src/client.cpp $(LDFLAGS) $(LDLIBS)

build/coordinator: src/coordinator.cpp
	$(CXX) -std=c++17 -O2 -g -Wall -o $@ src/coordinator.cpp -lpthread

$(PYMOD): src/rdmastorage.cpp src/client_lib.hpp src/common.hpp src/protocol.hpp src/ec.hpp
	$(CXX) $(CXXFLAGS) -shared -fPIC \
	    -I$(PYBIND_INC) $(PYTHON_INC) \
	    -o $@ src/rdmastorage.cpp $(LDFLAGS) $(LDLIBS)

clean:
	rm -rf build
