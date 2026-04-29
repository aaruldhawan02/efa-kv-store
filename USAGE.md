# efa-kv-store Usage Guide

RDMA-backed erasure-coded key-value store using libfabric + ISA-L.

---

## Build

```bash
make -j          # builds server, client, coordinator
make pymod       # builds rdmastorage Python module
```

Binaries land in `build/`. The Python module is `build/rdmastorage*.so`.

---

## Starting the System

### 1. Start the coordinator (on node0)

```bash
./build/coordinator
```

The coordinator listens on port 7777. It auto-assigns shard indices to servers as they register, computes k/m based on the number of alive nodes, and pushes failure notifications to connected clients.

**k/m assignment by node count:**

| Nodes | k | m | Tolerates |
|-------|---|---|-----------|
| 2     | 2 | 0 | 0 failures |
| 3     | 2 | 1 | 1 failure  |
| 4     | 3 | 1 | 1 failure  |
| 5     | 4 | 1 | 1 failure  |
| 6     | 4 | 2 | 2 failures |
| 7     | 5 | 2 | 2 failures |
| 8     | 6 | 2 | 2 failures |
| 9     | 6 | 3 | 3 failures |

### 2. Start servers (on each server node)

```bash
./build/server --coord node0
```

Run on each node (node1, node2, ...). Each server registers with the coordinator, which auto-assigns its shard index and monitors it with periodic PING/PONG heartbeats.

### 3. Verify registration

The coordinator prints each registration:
```
[coord] Server 0 registered. alive=1 → k=1 m=0
[coord] Server 1 registered. alive=2 → k=2 m=0
...
[coord] Server 5 registered. alive=6 → k=4 m=2
```

### Stopping

```bash
pkill coordinator
pkill server
```

---

## Failure Detection

The coordinator runs a SWIM-inspired failure detector: it sends `PING` to each server every 2 seconds over a persistent TCP connection. After 3 consecutive missed pings (~6 seconds), the server is declared dead.

When a server dies:
- Coordinator pushes a `DEAD <idx>` notification to all connected clients instantly
- Client's background thread marks that server as dead
- **GET** skips the dead server and uses degraded reads (erasure coding reconstructs from remaining shards)
- **PUT** fails immediately with an error (can't write all shards)

> **Note:** Clients already connected when a failure occurs can still read existing data via degraded reads. New clients connecting after a failure get the reconfigured k/m and connect to only the alive servers — existing data encoded under the old configuration is not accessible to new clients.

---

## CLI Client

```bash
./build/client --coord node0 <mode> [args]
```

k and m are assigned automatically by the coordinator — no flags needed.

### Modes

| Mode | Args | Description |
|------|------|-------------|
| `putfile` | `<key> <file>` | Store a file |
| `getfile` | `<key> <outfile>` | Retrieve a file |
| `delkey` | `<key>` | Delete a key |
| `put` | `<num_ops> <obj_bytes>` | Benchmark puts |
| `get` | `<num_ops> <obj_bytes>` | Benchmark gets |
| `bench` | `<num_ops> <obj_bytes>` | Benchmark put then get |

### Examples

```bash
# Store a file
./build/client --coord node0 putfile mykey photo.jpg

# Retrieve a file
./build/client --coord node0 getfile mykey photo_out.jpg

# Delete a key
./build/client --coord node0 delkey mykey

# Benchmark: 1000 puts of 64KB objects
./build/client --coord node0 bench 1000 65536
```

---

## Python Library

### Setup

```python
import sys
sys.path.insert(0, '/path/to/efa-kv-store/build')
import rdmastorage
```

### Connecting

```python
client = rdmastorage.Client('node0')
# or with explicit port:
client = rdmastorage.Client('node0', port=7777)

print(client.k, client.m)  # assigned by coordinator
```

Connections and memory registrations are established once and reused — no cold start on subsequent operations. A background thread listens for `DEAD` notifications from the coordinator.

### put / get / delete

```python
# Store bytes
client.put('mykey', b'hello world')

# Store a file
with open('photo.jpg', 'rb') as f:
    client.put('photo', f.read())

# Retrieve bytes
data = client.get('mykey')

# Retrieve and save a file
with open('photo_out.jpg', 'wb') as f:
    f.write(client.get('photo'))

# Delete
client.delete('mykey')
```

### Phase breakdown (latency profiling)

After a `put` or `get`, inspect where time was spent:

```python
client.put('key', data)
enc, ctrl_rtt, rdma_write, commit_rtt = client.last_put_phases()
print(f'encode={enc:.1f}us  ctrl_rtt={ctrl_rtt:.1f}us  rdma={rdma_write:.1f}us  commit={commit_rtt:.1f}us')

client.get('key')
decode, ctrl_rtt, rdma_read = client.last_get_phases()
print(f'ctrl_rtt={ctrl_rtt:.1f}us  rdma={rdma_read:.1f}us  decode={decode:.1f}us')
```

---

## Benchmarks

### Interactive notebook

```bash
jupyter notebook --no-browser --ip=0.0.0.0
```

Open `demo.ipynb` — covers basic ops, phase breakdown, throughput sweep, and failure detection demo.

### End-to-end latency sweep (256B → 1MB)

```bash
python3 bench_latency.py --coord node0 --ops 200
```

### Phase breakdown (isolate RDMA wire latency)

```bash
python3 bench_phases.py --coord node0 --ops 200 --size 65536
```

### Configurable workload benchmark

`bench_workload.py` runs named workloads across a range of object sizes and prints latency stats (avg, p50, p95, p99) plus bandwidth. It also prints a per-phase breakdown by default.

```bash
# Default: put_get workload, sizes 4KB/16KB/64KB/256KB, 200 ops each
python3 bench_workload.py --coord node0

# PUT-only workload, two sizes, 500 ops
python3 bench_workload.py --coord node0 --workload put_only --sizes 4096 65536 --ops 500

# PUT → GET → DELETE cycle
python3 bench_workload.py --coord node0 --workload put_get_delete

# Skip per-phase breakdown (faster output)
python3 bench_workload.py --coord node0 --no-phases
```

**Available workloads:**

| Workload | Operations |
|----------|------------|
| `put_only` | PUT only |
| `get_only` | PUT to populate, then GET only |
| `put_get` | Alternating PUT then GET (default) |
| `put_get_delete` | PUT → GET → DELETE cycle |

**All flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--coord` | `node0` | Coordinator hostname |
| `--port` | `7777` | Coordinator port |
| `--workload` | `put_get` | Workload type |
| `--sizes BYTES...` | `4096 16384 65536 262144` | Object sizes in bytes |
| `--ops N` | `200` | Operations per size |
| `--warmup N` | `20` | Warmup ops per size (excluded from stats) |
| `--no-phases` | off | Skip per-phase latency breakdown |

**Example output (64KB, put_get workload):**

```
--- 64KB ---
  op     size         avg        p50        p95        p99          bw
  -----------------------------------------------------------------------
  PUT     64KB  avg=  312.4us  p50=  305.1us  p95=  398.2us  p99=  441.0us   204.9 MB/s
         phase           avg       p50       p95       p99
         --------------------------------------------------------
         encode        12.3us    11.8us    15.2us    17.1us  (4%)
         ctrl RTT      48.7us    46.2us    61.4us    74.3us  (16%)
         RDMA write   241.5us   235.0us   312.8us   351.2us  (77%)
         commit RTT     9.9us     9.4us    13.1us    15.6us  (3%)
         total        312.4us   302.4us   402.5us   458.2us
  GET     64KB  avg=  198.6us  p50=  192.3us  p95=  251.7us  p99=  289.4us   322.4 MB/s
         phase           avg       p50       p95       p99
         --------------------------------------------------------
         ctrl RTT      38.1us    36.4us    48.2us    57.9us  (19%)
         RDMA read    152.3us   147.8us   196.4us   221.7us  (77%)
         decode         8.2us     7.9us    10.4us    12.3us  (4%)
         total        198.6us   192.1us   255.0us   291.9us
```

### Smoke tests

```bash
python3 test_rdmastorage.py --coord node0
```

---

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `coord port` | 7777 | Coordinator TCP port |
| `kPingIntervalMs` | 2000ms | How often coordinator pings servers |
| `kPingTimeoutMs` | 1000ms | Ping response deadline |
| `kMaxMissed` | 3 | Missed pings before declaring dead |
| `kPoolSize` | 512 MB | Per-server RDMA memory pool (`src/server.cpp`) |
| `kMaxSlots` | 65536 | Max concurrent keys per server (`src/server.cpp`) |
