#!/usr/bin/env bash
# run_matrix.sh — full 6 × 5 YCSB sweep for RDMAStorage.
#
# For each N in {1..6} and workload in {a, b, c, d, f}:
#   1. tear down any previous cluster (ssh + local pkill)
#   2. start coordinator locally + N servers via ssh
#   3. wait for the coordinator to report alive=N
#   4. ycsb load + ycsb run for the workload
#   5. parse the run log, append one row to results/runs.csv
#
# Configuration is via env vars at the top — edit or override on invocation:
#   REPO            project root (default: $HOME/RDMA-Distributed-KV-Store)
#   YCSB_HOME       stock ycsb-0.17.0 install (default: $REPO/ycsb-0.17.0)
#   COORD_NODE      hostname the coordinator runs on (default: node0)
#   SERVER_NODES    space-separated list of server hostnames (default: node1..node6)
#
# Run from $REPO:
#   bash benchmarks/run_matrix.sh
#
# Logs land in benchmarks/results/raw/; the aggregated CSV is at
# benchmarks/results/runs.csv. The script is idempotent — re-running just
# appends more rows.

set -u

REPO=${REPO:-$HOME/RDMA-Distributed-KV-Store}
YCSB_HOME=${YCSB_HOME:-$REPO/ycsb-0.17.0}
COORD_NODE=${COORD_NODE:-node0}
SERVER_NODES=(${SERVER_NODES:-node1 node2 node3 node4 node5 node6})

NS_TO_RUN=(${NS_TO_RUN:-1 2 3 4 5 6})
WORKLOADS=(${WORKLOADS:-a b c d f})
RECORDS=${RECORDS:-50000}
OPS=${OPS:-10000}
VALUE_BYTES=${VALUE_BYTES:-1024}
THREADS=${THREADS:-1}

RESULTS=$REPO/benchmarks/results
RAW=$RESULTS/raw
CSV=$RESULTS/runs.csv
mkdir -p "$RAW"

JVM_ARGS="-XX:TieredStopAtLevel=1 -XX:ErrorFile=/tmp/hs_err_%p.log"

log() { echo "[$(date +%T)] $*"; }

tear_down() {
    log "tear_down: killing coordinator + servers"
    pkill -9 -f build/coordinator 2>/dev/null || true
    for node in "${SERVER_NODES[@]}"; do
        ssh -o ConnectTimeout=5 -o BatchMode=yes "$node" \
            "pkill -9 -f build/server 2>/dev/null" </dev/null &
    done
    wait
    sleep 2
}

bring_up() {
    local N=$1
    local coord_log=$RAW/coord_N${N}.log
    rm -f "$coord_log"

    log "bring_up: coordinator on $COORD_NODE"
    nohup "$REPO/build/coordinator" > "$coord_log" 2>&1 &
    sleep 1

    log "bring_up: $N server(s)"
    for i in $(seq 0 $((N - 1))); do
        local node=${SERVER_NODES[$i]}
        ssh -o ConnectTimeout=5 -o BatchMode=yes "$node" \
            "cd $REPO && nohup ./build/server --coord $COORD_NODE > server.log 2>&1 &" \
            </dev/null &
    done
    wait

    log "bring_up: waiting for alive=$N (max 60s)"
    for _ in $(seq 1 60); do
        if grep -q "alive=$N " "$coord_log" 2>/dev/null; then
            log "bring_up: coordinator reports alive=$N"
            return 0
        fi
        sleep 1
    done
    log "ERROR: cluster did not reach alive=$N; last coord log:"
    tail -n 20 "$coord_log"
    return 1
}

run_cell() {
    local N=$1 W=$2
    local Wu
    Wu=$(echo "$W" | tr a-z A-Z)
    local load_log=$RAW/rdma_${Wu}_N${N}_load.log
    local run_log=$RAW/rdma_${Wu}_N${N}_run.log

    log "cell N=$N W=$Wu: load"
    "$YCSB_HOME/bin/ycsb" load rdmastorage \
        -jvm-args="$JVM_ARGS" \
        -P "$YCSB_HOME/workloads/workload$W" \
        -p rdmastorage.coord="$COORD_NODE" \
        -p recordcount="$RECORDS" \
        -p fieldcount=1 -p fieldlength="$VALUE_BYTES" \
        -threads "$THREADS" -s > "$load_log" 2>&1
    if ! grep -q '\[OVERALL\]' "$load_log"; then
        log "WARN: load N=$N W=$Wu failed; see $load_log"
        return 1
    fi

    log "cell N=$N W=$Wu: run"
    "$YCSB_HOME/bin/ycsb" run rdmastorage \
        -jvm-args="$JVM_ARGS" \
        -P "$YCSB_HOME/workloads/workload$W" \
        -p rdmastorage.coord="$COORD_NODE" \
        -p operationcount="$OPS" \
        -p fieldcount=1 -p fieldlength="$VALUE_BYTES" \
        -threads "$THREADS" -s > "$run_log" 2>&1
    if ! grep -q '\[OVERALL\]' "$run_log"; then
        log "WARN: run N=$N W=$Wu failed; see $run_log"
        return 1
    fi

    local coord_log=$RAW/coord_N${N}.log
    local K M
    K=$(grep "alive=$N " "$coord_log" | tail -1 | grep -oP 'k=\K\d+')
    M=$(grep "alive=$N " "$coord_log" | tail -1 | grep -oP 'm=\K\d+')
    K=${K:-N/A}
    M=${M:-N/A}

    python3 "$REPO/benchmarks/parse_ycsb_output.py" "$run_log" \
        --backend rdmastorage --workload "$Wu" \
        --servers "$N" --k "$K" --m "$M" \
        --threads "$THREADS" --value-bytes "$VALUE_BYTES" \
        --record-count "$RECORDS" \
        --csv "$CSV"
}

main() {
    log "matrix: NS_TO_RUN=(${NS_TO_RUN[*]}) WORKLOADS=(${WORKLOADS[*]})"
    log "matrix: RECORDS=$RECORDS OPS=$OPS VALUE_BYTES=$VALUE_BYTES THREADS=$THREADS"
    log "matrix: results -> $CSV"

    for N in "${NS_TO_RUN[@]}"; do
        tear_down
        if ! bring_up "$N"; then
            log "ERROR: skipping N=$N (bring_up failed)"
            continue
        fi
        for W in "${WORKLOADS[@]}"; do
            run_cell "$N" "$W" || log "WARN: cell N=$N W=$W produced no row, continuing"
        done
    done

    tear_down
    log "matrix: done. CSV at $CSV"
}

main "$@"
