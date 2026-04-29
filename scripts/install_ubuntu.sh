#!/bin/bash
# Build efa-kv-store on Ubuntu 22.04 with RoCE/verbs (CloudLab xl170 nodes).
# Requires Mellanox OFED or rdma-core already active on the NIC.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Installing build tools ==="
sudo apt-get update -y
sudo apt-get install -y build-essential git

echo "=== Installing libfabric (verbs provider) ==="
sudo apt-get install -y libfabric-dev

echo "=== Installing RDMA user-space libraries ==="
sudo apt-get install -y rdma-core libibverbs-dev librdmacm-dev ibverbs-utils

echo "=== Installing ISA-L (erasure coding) ==="
if ! apt-cache show libisal-dev &>/dev/null || ! sudo apt-get install -y libisal-dev 2>/dev/null; then
    echo "libisal-dev not in repos, building from source..."
    sudo apt-get install -y autoconf automake libtool nasm
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
fi

echo "=== Checking RDMA device ==="
ibv_devinfo || echo "WARNING: no RDMA device found — check that the NIC is RoCE-capable"
fi_info -p verbs 2>&1 | head -10 || echo "WARNING: fi_info -p verbs failed"

echo "=== Building ==="
cd "$PROJECT_DIR"
make clean
make -j"$(nproc)"
make pymod

echo ""
echo "=== Build complete ==="
echo ""
echo "See USAGE.md for startup instructions."
