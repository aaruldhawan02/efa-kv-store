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

The coordinator listens on port 7777 by default. It assigns shard indices to servers as they register and hands out addresses to clients.

### 2. Start servers (on each server node)

```bash
./build/server --coord node0
```

Run this on each node (node1, node2, ...). The server connects to the coordinator, registers itself, and waits for client connections.

For a k=4 m=2 cluster you need 6 servers running before the client can connect.

### 3. Verify all servers are registered

On node0, check the coordinator output — it prints each registration:
```
Registered shard 0: <addr>
Registered shard 1: <addr>
...
```

### Stopping

```bash
pkill coordinator
pkill server
```

---

## CLI Client

```bash
./build/client --coord node0 -k <k> [-m <m>] <mode> [args]
```

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
./build/client --coord node0 -k 4 -m 2 putfile mykey photo.jpg

# Retrieve a file
./build/client --coord node0 -k 4 -m 2 getfile mykey photo_out.jpg

# Delete a key
./build/client --coord node0 -k 4 -m 2 delkey mykey

# Benchmark: 1000 puts of 64KB objects
./build/client --coord node0 -k 4 -m 2 bench 1000 65536
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
client = rdmastorage.Client(coord="node0", k=4, m=2)
# or with explicit port:
client = rdmastorage.Client("node0", k=4, m=2, port=7777)
```

Connections are established once and reused for the lifetime of the object — no cold start on subsequent operations.

### put / get / delete

```python
# Store bytes
client.put("mykey", b"hello world")

# Store a file
with open("photo.jpg", "rb") as f:
    client.put("photo", f.read())

# Retrieve bytes
data = client.get("mykey")

# Retrieve and save a file
with open("photo_out.jpg", "wb") as f:
    f.write(client.get("photo"))

# Delete
client.delete("mykey")
```

### Phase breakdown (latency profiling)

After a `put` or `get`, you can inspect where time was spent:

```python
client.put("key", data)
enc, ctrl_rtt, rdma_write, commit_rtt = client.last_put_phases()
print(f"encode={enc:.1f}us  ctrl_rtt={ctrl_rtt:.1f}us  rdma={rdma_write:.1f}us  commit={commit_rtt:.1f}us")

client.get("key")
decode, ctrl_rtt, rdma_read = client.last_get_phases()
print(f"decode={decode:.1f}us  ctrl_rtt={ctrl_rtt:.1f}us  rdma={rdma_read:.1f}us")
```

---

## Benchmarks

### End-to-end latency sweep (256B → 1MB)

```bash
python3 bench_latency.py --coord node0 -k 4 -m 2 --ops 200
```

### Phase breakdown (isolate RDMA wire latency)

```bash
python3 bench_phases.py --coord node0 -k 4 -m 2 --ops 200 --size 65536
```

### Smoke tests

```bash
python3 test_rdmastorage.py --coord node0 -k 4 -m 2
```

---

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `k` | — | Number of data shards (required) |
| `m` | 1 | Number of parity shards (tolerated failures) |
| `port` | 7777 | Coordinator port |
| `kPoolSize` | 512 MB | Per-server RDMA memory pool (`src/server.cpp`) |
| `kMaxSlots` | 65536 | Max concurrent keys per server (`src/server.cpp`) |

With k=4 m=2: tolerates any 2 server failures. Needs 4 shards to reconstruct any object.
