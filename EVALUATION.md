# Evaluation Guide

All benchmarks require the cluster to be running first:

```bash
# node0
./build/coordinator

# node1, node2, ... (each storage node)
./build/server --coord node0
```

---

## bench_latency.py

PUT/GET latency sweep across object sizes (256 B → 1 MB).

```bash
python3 bench_latency.py --coord node0
python3 bench_latency.py --coord node0 --ops 1000
```

| Flag | Default | Description |
|------|---------|-------------|
| `--coord` | `node0` | Coordinator hostname |
| `--ops` | 200 | Operations per object size |

**Output:** avg, p50, p90, p99 latency and throughput (MB/s) for each size.

---

## bench_phases.py

Per-phase latency breakdown for a single object size. Shows how time splits across encode, control RTT, RDMA wire, and commit.

```bash
python3 bench_phases.py --coord node0
python3 bench_phases.py --coord node0 --size 65536 --ops 500
```

| Flag | Default | Description |
|------|---------|-------------|
| `--coord` | `node0` | Coordinator hostname |
| `--size` | 65536 | Object size in bytes |
| `--ops` | 200 | Number of operations |

**Output:** Per-phase avg/p50/p99 with percentage of total latency.

---

## bench_workload.py

Configurable mixed workloads (put only, get only, put+get, put+get+delete) across multiple object sizes with optional per-phase breakdown.

```bash
python3 bench_workload.py --coord node0 --workload put_get
python3 bench_workload.py --coord node0 --workload put_only --sizes 4096 65536 --ops 500
python3 bench_workload.py --coord node0 --workload put_get_delete --no-phases
```

| Flag | Default | Description |
|------|---------|-------------|
| `--coord` | `node0` | Coordinator hostname |
| `--workload` | `put_get` | `put_only`, `get_only`, `put_get`, `put_get_delete` |
| `--sizes` | `4096 16384 65536 262144` | Object sizes in bytes (space-separated) |
| `--ops` | 200 | Operations per size |
| `--warmup` | 20 | Warmup operations per size |
| `--no-phases` | off | Skip per-phase breakdown |

**Output:** Latency table per op per size, with optional phase breakdown.

---

## bench_ycsb.py

YCSB-style workloads with realistic Zipfian key distribution. Directly comparable to published YCSB results for other systems.

```bash
python3 bench_ycsb.py --coord node0 --workload A
python3 bench_ycsb.py --coord node0 --workload B --size 65536 --ops 1000
python3 bench_ycsb.py --coord node0 --workload C --dist uniform
```

| Flag | Default | Description |
|------|---------|-------------|
| `--coord` | `node0` | Coordinator hostname |
| `--workload` | `A` | YCSB workload A/B/C/D (see table below) |
| `--size` | 65536 | Object size in bytes |
| `--ops` | 500 | Number of measured operations |
| `--warmup` | 50 | Warmup operations |
| `--keys` | 1000 | Key-space size |
| `--dist` | `zipfian` | Key distribution: `zipfian` or `uniform` |
| `--no-phases` | off | Skip per-phase breakdown |

**Workload definitions:**

| Workload | Read | Update | Insert | Description |
|----------|------|--------|--------|-------------|
| A | 50% | 50% | — | Heavy update |
| B | 95% | 5% | — | Mostly reads |
| C | 100% | — | — | Read only |
| D | 95% | — | 5% | Read latest |

**Output:** Operation counts, avg/p50/p95/p99 latency for read and write separately.

---

## failure_demo.py

Live failure detection and degraded read demo. Stores a key, gives you 30 seconds to kill a server node, then verifies the key is still readable after the DEAD notification arrives.

```bash
python3 failure_demo.py
```

No flags — hardcoded to `node0`. During the countdown, kill a server on any other node:

```bash
pkill server   # run on the node you want to kill
```

**Output:** Confirms DEAD notification received, then retrieves the key via degraded read using surviving shards.

---

## test_rdmastorage.py

Correctness smoke test. Verifies PUT/GET/DELETE for various sizes and checks data integrity. Run this first on a fresh cluster to confirm everything is working before benchmarking.

```bash
python3 test_rdmastorage.py --coord node0
```

| Flag | Default | Description |
|------|---------|-------------|
| `--coord` | `node0` | Coordinator hostname |
| `--port` | 7777 | Coordinator port |

**Output:** Pass/fail for each test case, plus basic throughput numbers.
