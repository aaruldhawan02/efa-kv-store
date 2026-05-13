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

# JNI module for real Java YCSB. JAVA_HOME is auto-detected from `which javac`
# if not already set.
JAVA_HOME ?= $(shell readlink -f $$(which javac 2>/dev/null) 2>/dev/null | sed 's|/bin/javac||')
JNI_INC   := -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux
JNIMOD    := build/librdmastorage_jni.so
JNI_DIR   := benchmarks/binding/rdmastorage
JNIJAR    := $(JNI_DIR)/target/rdmastorage-binding-0.1.0.jar

BINARIES = build/server build/client build/coordinator

.PHONY: all clean pymod jnimod jnijar ycsb

all: build $(BINARIES)

pymod: build $(PYMOD)

jnimod: build $(JNIMOD)

jnijar: $(JNIJAR)

ycsb: jnimod jnijar

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

$(JNIMOD): src/rdmastorage_jni.cpp src/client_lib.hpp src/common.hpp src/protocol.hpp src/ec.hpp
	@if [ -z "$(JAVA_HOME)" ] || [ ! -d "$(JAVA_HOME)/include" ]; then \
	    echo "ERROR: JAVA_HOME not set or invalid (got '$(JAVA_HOME)'). Install a JDK or pass JAVA_HOME=/path/to/jdk."; \
	    exit 1; \
	fi
	$(CXX) $(CXXFLAGS) $(JNI_INC) -shared -fPIC \
	    -o $@ src/rdmastorage_jni.cpp $(LDFLAGS) $(LDLIBS)

$(JNIJAR): $(JNI_DIR)/pom.xml $(shell find $(JNI_DIR)/src -name '*.java' 2>/dev/null)
	cd $(JNI_DIR) && mvn package -q

clean:
	rm -rf build
	cd $(JNI_DIR) && mvn clean -q 2>/dev/null || true
