#!/bin/bash
# Build efa-kv-store on EC2 Rocky Linux 9.
# Requires AWS EFA driver already installed at /opt/amazon/efa.
# No DAOS or external storage dependencies.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Installing build tools ==="
sudo dnf install -y gcc-c++ make

echo "=== Installing ISA-L (erasure coding) ==="
sudo dnf install -y epel-release
sudo dnf install -y isa-l-devel || {
    echo "isa-l-devel not found via dnf, building from source..."
    if ! command -v nasm &>/dev/null; then
        sudo dnf install -y nasm
    fi
    TMP=$(mktemp -d)
    pushd "$TMP" > /dev/null
    curl -fsSL https://github.com/intel/isa-l/archive/refs/tags/v2.31.0.tar.gz | tar xz
    cd isa-l-2.31.0
    ./autogen.sh
    ./configure --prefix=/usr/local
    make -j"$(nproc)"
    sudo make install
    sudo ldconfig
    popd > /dev/null
    rm -rf "$TMP"
}

echo "=== Verifying EFA driver ==="
if [[ ! -f /opt/amazon/efa/lib64/libfabric.so ]]; then
    echo "ERROR: /opt/amazon/efa/lib64/libfabric.so not found."
    echo "Install the AWS EFA driver first:"
    echo "  curl -O https://efa-installer.amazonaws.com/aws-efa-installer-latest.tar.gz"
    echo "  tar xf aws-efa-installer-latest.tar.gz && cd aws-efa-installer"
    echo "  sudo ./efa_installer.sh -y"
    exit 1
fi

if [[ ! -f /opt/amazon/efa/include/rdma/fabric.h ]]; then
    echo "WARNING: EFA headers not at /opt/amazon/efa/include/rdma/fabric.h"
    echo "Your EFA installer may be older. Try reinstalling with --enable-gdr flag."
    echo "Falling back to system libfabric-devel for headers..."
    sudo dnf install -y libfabric-devel || true
    # Override EFA_PREFIX to use system headers but EFA lib at runtime
    export EFA_HEADERS=/usr
    sed -i "s|EFA_PREFIX ?= /opt/amazon/efa|EFA_PREFIX ?= /opt/amazon/efa\nCXXFLAGS += -I/usr/include|" \
        "$PROJECT_DIR/Makefile" 2>/dev/null || true
fi

echo "=== EFA provider sanity check ==="
LD_LIBRARY_PATH=/opt/amazon/efa/lib64 fi_info -p efa 2>&1 | head -6 || \
    echo "WARNING: fi_info -p efa failed — check EFA driver installation"

echo "=== Building ==="
cd "$PROJECT_DIR"
make clean
make -j"$(nproc)"

echo ""
echo "=== Build complete ==="
echo ""
echo "SERVER (run on each server node):"
echo "  cd $(basename "$PROJECT_DIR") && ./build/server"
echo ""
echo "CLIENT (run on client node, paste server address(es) from above):"
echo "  ./build/client <addr0> <addr1> <addr2> bench 1000 65536"
echo ""
echo "Object sizes to benchmark: 256 4096 65536 262144 1048576"
