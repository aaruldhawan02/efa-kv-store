# Benchmarks

Real YCSB driving both RDMAStorage (via a JNI binding in this repo) and S3 Express One Zone (via YCSB's upstream `s3` binding). Same harness, same workload generator, same output format on both sides.

## Goal

Show how RDMAStorage scales across cluster size and workload mix, and how it stacks up against a commercial low-latency object store.

- **Workloads**: A, B, C, D, F (skip E — RDMAStorage has no scans)
- **Cluster size sweep**: 1, 2, 3, 4, 5, 6 storage servers
- **Baseline**: S3 Express One Zone, single-AZ, run from an in-AZ EC2 client
- **Output**: a single `results/runs.csv`, plotted by `plot.py`

## Run matrix

| Backend | Servers | Workloads | Ops/run | Records | Value size |
|---|---|---|---|---|---|
| RDMAStorage | 1, 2, 3, 4, 5, 6 | A, B, C, D, F | 10,000 | 50,000 | 1 KB |
| S3 Express  | n/a | A, B, C, D, F | 10,000 | 50,000 | 1 KB |

Total RDMAStorage cells: 6 × 5 = 30. Budget ~5–10 min/cell → 3–5 hours wall clock.

### Why skip Workload E

E is range scans. Our protocol ([../src/protocol.hpp](../src/protocol.hpp)) is point-lookup only. The JNI binding's `scan()` returns `Status.NOT_IMPLEMENTED`. Reporting E for one backend but not the other is misleading, so it's dropped from the comparison.

### Workload F (read-modify-write)

YCSB's standard implementation: `read` then `update` of the same key. Non-atomic. Neither RDMAStorage nor S3 Express offers compare-and-swap on a single object, so the comparison stays fair.

### k/m varies with server count — label it on the chart

The coordinator picks erasure-code parameters from the live server count:

| N | k | m | Tolerates |
|---|---|---|---|
| 1 | 1 | 0 | 0 |
| 2 | 2 | 0 | 0 |
| 3 | 2 | 1 | 1 |
| 4 | 3 | 1 | 1 |
| 5 | 4 | 1 | 1 |
| 6 | 4 | 2 | 2 |

The throughput-vs-N curve mixes two effects: more parallel servers *and* a different (k, m) configuration. Annotate each point with (k, m) so the curve is interpretable.

## Results schema

Each run appends one row to `results/runs.csv`:

```
backend,workload,servers,k,m,threads,value_bytes,record_count,op_count,throughput_ops,avg_us,p95_us,p99_us,run_id,notes
```

- `backend` ∈ {`rdmastorage`, `s3express`}
- `servers`, `k`, `m` are `N/A` for S3 rows
- `run_id` is a timestamp like `20260512T1402`
- `notes` records context — `ec2-in-az`, `cloudlab-wan`, threading mode, etc.

A small `parse_ycsb_output.py` (to be added) converts each YCSB stdout summary into one CSV row.

## Plotting plan

Three charts from the single CSV, emitted as PNGs into `results/`:

1. **Throughput vs. server count** — X: N (1–6), Y: ops/sec, one line per workload {A,B,C,D,F}. RDMAStorage only. Annotate each marker with (k, m).
2. **p99 latency vs. server count** — same shape, Y: p99 µs.
3. **RDMAStorage vs S3 Express** — grouped bar per workload, RDMAStorage@N=6 vs S3 Express. Two bars per workload. Label each bar with p99.

---

# Part 1: RDMAStorage with YCSB (JNI binding)

## Architecture

```
+-----------------+      JNI       +-------------------+   libfabric/RDMA   +----------+
| YCSB Java       | -------------> | librdmastorage_   | -----------------> | server 0 |
| CoreWorkload    |  byte[] in/out | jni.so (C++)      |                    | server 1 |
| + DBWrapper     |                | wraps ErasureClient|                   |    ...   |
+-----------------+                +-------------------+                    +----------+
        ^                                                                        |
        |  rdmastorage-binding-0.1.0.jar                                          |
        |  (RdmaStorageClient extends DB)                                         |
        +-------- coordinator (TCP, port 7777) -----------------------------------+
```

YCSB's CoreWorkload generates ops and keys; `RdmaStorageClient` translates each `read/insert/update/delete` into a JNI call into the existing `ErasureClient`. No Python in the path.

The binding lives at [binding/rdmastorage/](binding/rdmastorage/) inside this folder. The JNI shim is at [../src/rdmastorage_jni.cpp](../src/rdmastorage_jni.cpp).

## Prerequisites

- Built binaries: `make -j && make pymod` produces `build/coordinator`, `build/server`
- JDK 8+ (`javac --version`)
- Maven 3.6+ (`mvn --version`)
- 7 CloudLab xl170 nodes with hostnames `node0..node6` reachable over ssh
- libfabric verbs provider — verify with `fi_info -p verbs`

## Build the binding

```bash
make ycsb                 # builds both build/librdmastorage_jni.so and the JAR
# equivalently:
make jnimod               # JNI shared library
make jnijar               # Maven-built JAR under benchmarks/binding/rdmastorage/target/
```

`JAVA_HOME` is auto-detected from `which javac`. Override if needed:

```bash
make JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 ycsb
```

## Drop the binding into a stock YCSB install

YCSB doesn't need to be rebuilt, but the upstream 0.17.0 release needs three small one-time tweaks: the wrapper script is Python 2, it doesn't know about our binding name, and it expects each binding's JAR to live in its own subdirectory.

```bash
# One-time: download stock YCSB
wget https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz
tar xzf ycsb-0.17.0.tar.gz
export YCSB_HOME=$PWD/ycsb-0.17.0

# (1) The bin/ycsb wrapper is Python 2 only. Install py2 and pin the shebang.
sudo apt update && sudo apt install -y python2.7
sudo sed -i '1c #!/usr/bin/env python2.7' $YCSB_HOME/bin/ycsb

# (2) Register "rdmastorage" in the wrapper's DATABASES dict so it accepts the
#     short name as a positional arg (the wrapper rejects FQCNs).
sudo sed -i '/^DATABASES = {/a\    "rdmastorage"     : "site.ycsb.db.rdmastorage.RdmaStorageClient",' $YCSB_HOME/bin/ycsb

# (3) The wrapper looks for binding JARs at $YCSB_HOME/<name>-binding/lib/*.jar.
mkdir -p $YCSB_HOME/rdmastorage-binding/lib
cp benchmarks/binding/rdmastorage/target/rdmastorage-binding-0.1.0.jar \
   $YCSB_HOME/rdmastorage-binding/lib/

# Tell the JVM and the dynamic linker where our native libs are.
export LD_LIBRARY_PATH=$PWD/build:$LD_LIBRARY_PATH
export JAVA_OPTS="-Djava.library.path=$PWD/build"
```

`LD_LIBRARY_PATH` is so the JNI `.so` can find `libfabric.so` and `libisal.so` at load time. `java.library.path` is so the JVM finds `librdmastorage_jni.so` for `System.loadLibrary("rdmastorage_jni")`.

If you'd rather not edit the wrapper, you can bypass it entirely:

```bash
JAR=$PWD/benchmarks/binding/rdmastorage/target/rdmastorage-binding-0.1.0.jar
java -Djava.library.path=$PWD/build \
     -cp "$YCSB_HOME/lib/*:$JAR" \
     site.ycsb.Client -load \
     -db site.ycsb.db.rdmastorage.RdmaStorageClient \
     -P $YCSB_HOME/workloads/workloada \
     ...
```

`-load` runs the load phase, `-t` runs the transaction phase — that's what the wrapper translates to under the hood.

## Why `-XX:TieredStopAtLevel=1`

The HotSpot C2 (server) JIT compiler in OpenJDK 11 generates code that causes a stack-corruption SIGSEGV when invoking our JNI methods through `DBWrapper` — observed via gdb as a return address overwritten with a JVM heap pointer. C1 (the simpler client compiler) does not trigger the bug. `-XX:TieredStopAtLevel=1` keeps JIT enabled but stops at level 1 (C1 only), avoiding C2's aggressive optimizations. Performance impact in our measurements: C1-only is ~2× faster than `-Xint` and within a small constant of the fully-optimized path. The full investigation lives in TODO; for the benchmark numbers the C1-only flag is in effect.

## Cluster layout

**node0 = coordinator + YCSB client.** **node1..nodeN = storage servers.** Putting the client on the coordinator node keeps the data path symmetric across all servers and avoids loopback RDMA skew.

## Single-cell procedure

Bring up:

```bash
# node0
./build/coordinator
```

```bash
# node1..nodeN, in parallel (zellij pane per node is convenient)
./build/server --coord node0
```

Wait for the coordinator to print `alive=N → k=K m=M`. Verify against the table above.

Make sure `$N` is set and the output dir exists:

```bash
mkdir -p benchmarks/results/raw
export N=6   # match the live server count
```

Load phase (insert the dataset):

```bash
# node0
$YCSB_HOME/bin/ycsb load rdmastorage \
    -jvm-args="-XX:TieredStopAtLevel=1 -XX:ErrorFile=/tmp/hs_err_%p.log" \
    -P $YCSB_HOME/workloads/workloada \
    -p rdmastorage.coord=node0 \
    -p recordcount=50000 \
    -p fieldcount=1 \
    -p fieldlength=1024 \
    -threads 1 \
    -s 2>&1 | tee benchmarks/results/raw/rdma_A_N${N}_load.log
```

Run phase:

```bash
$YCSB_HOME/bin/ycsb run rdmastorage \
    -jvm-args="-XX:TieredStopAtLevel=1 -XX:ErrorFile=/tmp/hs_err_%p.log" \
    -P $YCSB_HOME/workloads/workloada \
    -p rdmastorage.coord=node0 \
    -p operationcount=10000 \
    -p fieldcount=1 \
    -p fieldlength=1024 \
    -threads 1 \
    -s 2>&1 | tee benchmarks/results/raw/rdma_A_N${N}_run.log
```

Tear down between cells:

```bash
# every server node
pkill -f build/server
# node0
pkill -f build/coordinator
```

Always tear down — a stale server registration leaves indices misaligned and the next run will silently use the wrong k/m.

## Threading and concurrency

YCSB's `-threads N` creates N worker threads, each with its own `RdmaStorageClient` instance (so N separate libfabric connections). Verify that `ErasureClient` is safe to instantiate concurrently — if not, run `-threads 1` per process and spawn multiple YCSB processes with disjoint key ranges instead.

For the primary numbers, run `-threads 1` (latency-bound) and `-threads 16` (throughput-bound). Report both rows in the CSV.

## Sweep script

`benchmarks/run_matrix.sh` (to be added) does the full 6×5 sweep:

```bash
for N in 1 2 3 4 5 6; do
    bring_up_cluster $N
    wait_for_alive_count $N
    for W in a b c d f; do
        $YCSB_HOME/bin/ycsb load rdmastorage -P workload$W ... >  raw/rdma_${W}_N${N}_load.log
        $YCSB_HOME/bin/ycsb run  rdmastorage -P workload$W ... > raw/rdma_${W}_N${N}_run.log
        python3 benchmarks/parse_ycsb_output.py raw/rdma_${W}_N${N}_run.log \
            --backend rdmastorage --workload $W --servers $N >> results/runs.csv
    done
    tear_down_cluster
done
```

## Tips and gotchas

- **NUMA pinning**: `taskset -c 0-15 $YCSB_HOME/bin/ycsb ...` — pin to cores on the same NUMA node as the RDMA NIC.
- **Verify k/m before each run**: tail the coordinator output, or add a `--status` flag to the coordinator that prints `k m alive` and exits.
- **Don't include load phase in the throughput number**: the YCSB summary already separates `[LOAD]` from the run-phase `[OVERALL]`. Only parse the run output.
- **Warmup**: YCSB doesn't have a built-in warmup. Run a short throwaway run-phase first (~1,000 ops) before the measured run, or take the first 10% of operationcount as warmup and parse it separately.

---

# Part 2: S3 Express baseline

## Why S3 Express

- Designed for single-AZ, high-throughput, low-latency object storage — single-digit-ms reads inside the AZ.
- Closest commercial peer to an RDMA-backed KV in *intended use case*.
- Standard S3 (regional) is a much weaker baseline — its 30–100 ms tail latency makes RDMA look artificially great. Express is the harder, more honest target.

## Where to run YCSB from

This decision dominates the numbers:

| Option | What it tests | Pros | Cons |
|---|---|---|---|
| **YCSB on CloudLab node0** | Same client hardware as RDMAStorage runs | Controls for client compute | ~30–50 ms public-internet RTT to AWS dominates — measures the WAN, not the store |
| **YCSB on an EC2 instance in same AZ as the bucket** | What S3 Express was built for | Single-digit-ms latency, fair to AWS | Different client hardware than RDMAStorage runs |

**Recommendation: EC2 in-AZ.** The point of a baseline is to show what a production system delivers under the conditions it was *designed* for. Label both setups clearly in the chart.

## Build YCSB's S3 binding

The upstream binding pins an older AWS SDK. S3 Express directory buckets need **AWS SDK for Java v1 ≥ 1.12.561**. You may need to bump `aws-java-sdk-s3` in `s3/pom.xml` before building.

```bash
# On the EC2 client
git clone https://github.com/brianfrankcooper/YCSB.git
cd YCSB
# Edit s3/pom.xml: bump aws-java-sdk-s3 to a version that supports directory buckets
mvn -pl site.ycsb:s3-binding -am clean package -DskipTests
```

## Create the bucket

```bash
aws s3api create-bucket \
    --bucket rdma-baseline--use1-az5--x-s3 \
    --create-bucket-configuration 'Location={Type=AvailabilityZone,Name=use1-az5},Bucket={Type=Directory,DataRedundancy=SingleAvailabilityZone}' \
    --region us-east-1
```

Directory-bucket naming: `<name>--<az>--x-s3`.

## Load + run

```bash
./bin/ycsb load s3 -P workloads/workloada \
    -p s3.bucket=rdma-baseline--use1-az5--x-s3 \
    -p s3.region=us-east-1 \
    -p s3.endpoint=https://s3express-use1-az5.us-east-1.amazonaws.com \
    -p recordcount=50000 \
    -p fieldcount=1 \
    -p fieldlength=1024 \
    -threads 16 -s 2>&1 | tee results/raw/s3_A_load.log

./bin/ycsb run s3 -P workloads/workloada \
    -p s3.bucket=rdma-baseline--use1-az5--x-s3 \
    -p s3.region=us-east-1 \
    -p s3.endpoint=https://s3express-use1-az5.us-east-1.amazonaws.com \
    -p operationcount=10000 \
    -p fieldcount=1 \
    -p fieldlength=1024 \
    -threads 16 -s 2>&1 | tee results/raw/s3_A_run.log
```

The final summary block contains `[OVERALL]`, `[READ]`, `[UPDATE]` etc. with `Throughput(ops/sec)`, `AverageLatency`, `95thPercentileLatency`, `99thPercentileLatency` — `parse_ycsb_output.py` converts that into a CSV row.

## Concurrency

S3 Express is throughput-optimized. Single-threaded ≈ 200 ops/sec — latency-bound. With `-threads 16` or `-threads 64`, throughput scales linearly until you hit the client's NIC limit. Report both `threads=1` and `threads=16` rows so the chart can show either dimension.

## Cost

S3 Express in us-east-1 (verify on AWS pricing):

- ~$0.0025 per 1,000 PUTs
- ~$0.0002 per 1,000 GETs

30 runs × ~20k ops ≈ 600k ops. Well under $5. Plus a couple hours of `c7gn.xlarge` ≈ a couple dollars. Tag the bucket and tear it down after.

---

## bench_ycsb.py — Python dev tool

[bench_ycsb.py](bench_ycsb.py) is a Python-only port of a YCSB workload runner against the pybind `rdmastorage` module. **It is not the benchmark of record** — that's Java YCSB above. Keep it around for:

- Fast dev-loop iteration when the JNI binding is broken or the JVM is annoying
- Sanity-checking that the cluster behaves correctly before paying the YCSB Java setup cost
- Reproducing the per-phase latency breakdown (encode / ctrl RTT / RDMA / commit RTT), which YCSB can't see because it sits above the JNI boundary

The Python harness writes its summary in the same CSV schema as YCSB output, but flag any rows produced by it with `notes=python-harness` so they're not mixed into the headline numbers by accident.

---

## Files (this folder)

| File | Purpose |
|---|---|
| `README.md` | This document |
| `binding/rdmastorage/` | Maven module + Java DB class for real YCSB |
| `bench_ycsb.py` | Python dev-loop harness (not the benchmark of record) |
| `workloads/` | Optional local copy of YCSB workload property files |
| `results/raw/` | Raw YCSB stdout logs |
| `results/runs.csv` | Aggregated rows for plotting |
| `parse_ycsb_output.py` | YCSB stdout → CSV row converter (to be added) |
| `run_matrix.sh` | 6×5 sweep wrapper (to be added) |
| `plot.py` | Renders the three charts from `runs.csv` (to be added) |
