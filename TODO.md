# TODO

## 1. Evaluation & Benchmarking

**Goal:** Produce latency and throughput numbers that demonstrate the value of the RDMA + erasure coding design.

- [ ] **End-to-end latency sweep** — measure PUT and GET latency across object sizes (256B, 1KB, 4KB, 16KB, 64KB, 256KB, 1MB) with varying k+m configurations
- [ ] **Phase breakdown** — report encode, ctrl RTT, RDMA wire, commit RTT separately so RDMA wire latency is isolated from software overhead
- [ ] **Throughput sweep** — saturate the NIC; report MB/s vs. object size for both PUT and GET
- [ ] **Degraded read overhead** — compare normal GET vs. degraded GET (one server dead, parity reconstruction) latency and throughput
- [ ] **Scaling study** — vary number of server nodes (2–9) and report how throughput scales with k
- [ ] **Comparison baseline** — run same workload over TCP (loopback or same network) to quantify RDMA benefit

Scripts to add/extend: `bench_latency.py`, `bench_phases.py`, `bench_throughput.py`

---

## 2. Multiple Client Connections per Server

**Current limitation:** Each server calls `fi_accept()` once and then handles exactly one client for its lifetime. A second client connecting (e.g., new `rdmastorage.Client`) will hang or fail because the server never calls `fi_accept()` again.

**What needs to change:**

- [ ] **Server accept loop** — after a client disconnects (CQ error / EOF on ctrl socket), call `fi_accept()` again to wait for the next client
- [ ] **Concurrent clients** — for true multi-client support, each accepted endpoint needs its own thread (or event loop), its own CQ, and its own registered-memory view so clients don't share slot state
- [ ] **Client disconnect detection** — server must detect when a client closes its RDMA endpoint (libfabric `FI_SHUTDOWN` event on EQ) and clean up that client's slot reservations
- [ ] **Slot ownership** — slots currently have no client tag; add a client ID so a crashing client's unreleased slots can be reclaimed on reconnect

**Files:** `src/server.cpp` (accept loop, per-client thread), `src/client_lib.hpp` (reconnect logic if needed)

---

## 3. Server Data Expandability

**Current limitation:** The memory pool is a single 512 MB `mmap` slab registered with libfabric at startup (`kPoolSize = 512 MB`, `kMaxSlots = 65536`). Once the slab is full, PUT returns "pool memory exhausted" with no recovery path.

**What needs to change:**

- [ ] **Dynamic slab growth** — when the pool is exhausted, `mmap` a new slab, register it with `fi_mr_reg`, and add it to a free-list. Clients receive the new MR key/offset for writes into the new slab
- [ ] **Per-key slot reclamation** — DELETE currently marks a slot free in the bitmap; verify the slot is actually reused on subsequent PUTs (check `alloc_slot` / `free_slot` logic)
- [ ] **Variable-size slots** — current design wastes space for small objects (slot = `kMaxShardSize` regardless of actual shard size). Pack small objects into sub-slots or use a size-class allocator
- [ ] **Spill to local NVMe** — for objects that don't fit in registered memory, serialize shards to local SSD and serve reads from disk (falling back from RDMA to a read(2) + send path); only activate when RDMA pool pressure is high
- [ ] **Capacity reporting** — expose pool utilization (slots used / total, bytes used / total) via the coordinator's LIST response or a new STATUS command so clients can shed load before hitting exhaustion

**Files:** `src/server.cpp` (`PoolAllocator`, `alloc_slot`, `free_slot`), `src/coordinator.cpp` (STATUS command)
