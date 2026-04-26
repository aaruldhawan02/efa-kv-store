#!/bin/bash
# Install dependencies and build efa-kv-store on EC2 Rocky Linux 9 with EFA
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Installing build tools ==="
sudo dnf install -y gcc-c++ make

echo "=== Verifying EFA installation ==="
if [[ ! -f /opt/amazon/efa/lib64/libfabric.so ]]; then
    echo "ERROR: /opt/amazon/efa/lib64/libfabric.so not found."
    echo "Install the AWS EFA driver first: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-start.html"
    exit 1
fi

if [[ ! -f /opt/amazon/efa/include/rdma/fabric.h ]]; then
    echo "EFA headers not found at /opt/amazon/efa/include. Checking system..."
    # The EFA installer may put headers elsewhere; check common paths
    if [[ -f /usr/include/rdma/fabric.h ]]; then
        echo "Found system libfabric headers at /usr/include"
        echo "Building against system headers + EFA runtime..."
        # Override EFA_PREFIX to use system headers but EFA lib
        export EFA_PREFIX_INC=/usr/include
    else
        echo "No libfabric headers found. Installing libfabric-devel..."
        sudo dnf install -y libfabric-devel 2>/dev/null || \
            sudo dnf install -y daos-client 2>/dev/null || \
            { echo "Could not install libfabric-devel. Install EFA SDK."; exit 1; }
    fi
fi

echo "=== EFA provider check ==="
LD_LIBRARY_PATH=/opt/amazon/efa/lib64 fi_info -p efa 2>&1 | head -5 || true

echo "=== Building ==="
cd "$PROJECT_DIR"
make clean
make -j"$(nproc)"

echo ""
echo "=== Build complete ==="
echo "  Server: $PROJECT_DIR/build/server"
echo "  Client: $PROJECT_DIR/build/client"
echo ""
echo "Usage:"
echo "  Node 1 (server):  ./build/server"
echo "  Node 2 (client):  ./build/client <server_efa_addr> bench 10000 4096"
