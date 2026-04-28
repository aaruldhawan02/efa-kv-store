#!/bin/bash
# run.sh — start N servers on node1..nodeN, collect addresses, run client benchmark.
#
# Usage: ./run.sh <n_servers> <mode> <num_ops> <obj_bytes>
#   n_servers : number of server nodes (servers run on node1..nodeN)
#   mode      : put | get | bench
#   num_ops   : number of operations
#   obj_bytes : object size in bytes
#
# Examples:
#   ./run.sh 3 bench 10000 65536     # k=2 m=1, 10k ops, 64KB objects
#   ./run.sh 6 bench 10000 65536     # k=5 m=1, 10k ops, 64KB objects

set -euo pipefail

N="${1:-3}"
MODE="${2:-bench}"
NUM_OPS="${3:-1000}"
OBJ_BYTES="${4:-65536}"

BINARY="$(dirname "$0")/build/server"
CLIENT="$(dirname "$0")/build/client"

if [[ ! -x "$CLIENT" ]]; then
    echo "ERROR: $CLIENT not found — run 'make -j' first" >&2
    exit 1
fi

# Build the list of server host names node1..nodeN.
NODES=()
for i in $(seq 1 "$N"); do
    NODES+=("node${i}")
done

echo "Starting $N servers on: ${NODES[*]}"

# Temporary files for each server's address output.
TMPDIR_RUN=$(mktemp -d)
trap 'rm -rf "$TMPDIR_RUN"; kill $(jobs -p) 2>/dev/null || true' EXIT

ADDR_FILES=()
PIDS=()
for HOST in "${NODES[@]}"; do
    AFILE="$TMPDIR_RUN/${HOST}.addr"
    ADDR_FILES+=("$AFILE")
    ssh -o StrictHostKeyChecking=no "$HOST" \
        "cd ~/efa-kv-store && ./build/server" > "$AFILE" 2>&1 &
    PIDS+=($!)
done

# Wait until every server prints its "address: <hex>" line.
ADDRS=()
for AFILE in "${ADDR_FILES[@]}"; do
    DEADLINE=$(( $(date +%s) + 15 ))
    while true; do
        if [[ $(date +%s) -gt $DEADLINE ]]; then
            echo "ERROR: timed out waiting for server address in $AFILE" >&2
            exit 1
        fi
        LINE=$(grep -m1 "^address:" "$AFILE" 2>/dev/null || true)
        if [[ -n "$LINE" ]]; then
            ADDR=$(echo "$LINE" | awk '{print $2}')
            ADDRS+=("$ADDR")
            break
        fi
        sleep 0.2
    done
done

echo "Collected ${#ADDRS[@]} addresses: ${ADDRS[*]}"
echo ""

"$CLIENT" "${ADDRS[@]}" "$MODE" "$NUM_OPS" "$OBJ_BYTES"
