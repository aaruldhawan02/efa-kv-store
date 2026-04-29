#!/usr/bin/env python3
"""
Latency benchmark for rdmastorage.
Measures put/get latency across object sizes with warmup to avoid cold start.

Usage:
    python3 bench_latency.py [--coord node0] [-k 2] [-m 1] [--ops 1000]
"""

import sys
import os
import time
import argparse
import random
import statistics

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "build"))

try:
    import rdmastorage
except ImportError as e:
    print(f"ERROR: could not import rdmastorage: {e}")
    print("Build with: make pymod")
    sys.exit(1)


OBJECT_SIZES = [
    256,
    1024,
    4 * 1024,
    16 * 1024,
    64 * 1024,
    256 * 1024,
    1024 * 1024,
]

WARMUP_OPS = 20


def human_size(b):
    if b >= 1024 * 1024:
        return f"{b // (1024*1024)}MB"
    if b >= 1024:
        return f"{b // 1024}KB"
    return f"{b}B"


def percentile(sorted_data, p):
    idx = int(len(sorted_data) * p / 100)
    return sorted_data[min(idx, len(sorted_data) - 1)]


def bench(client, op, obj_bytes, num_ops, data=None):
    """Returns sorted list of latencies in microseconds."""
    if data is None:
        data = bytes(random.getrandbits(8) for _ in range(obj_bytes))

    # Use fixed key names so each run overwrites the previous size's data,
    # keeping pool usage bounded to num_ops slots at any given time.
    keys = [f"bench-{i:06d}" for i in range(num_ops)]

    # Warmup: pre-populate the exact keys we'll measure
    for key in keys[:WARMUP_OPS]:
        client.put(key, data)
        if op == "get":
            client.get(key)

    # Measure
    latencies = []
    for key in keys:
        if op == "put":
            t0 = time.perf_counter()
            client.put(key, data)
            latencies.append((time.perf_counter() - t0) * 1e6)
        else:
            client.put(key, data)
            t0 = time.perf_counter()
            client.get(key)
            latencies.append((time.perf_counter() - t0) * 1e6)

    latencies.sort()
    return latencies


def print_row(label, size_str, lats, num_ops):
    avg  = statistics.mean(lats)
    p50  = percentile(lats, 50)
    p99  = percentile(lats, 99)
    p999 = percentile(lats, 99.9)
    bw   = (num_ops * int(size_str.replace("MB","").replace("KB","").replace("B","")) /
            (sum(lats) / 1e6) / 1e6
            if False else 0)  # computed below via total_bytes
    print(f"  {label:<4}  {size_str:>6}  avg={avg:7.1f}us  "
          f"p50={p50:7.1f}us  p99={p99:7.1f}us  p999={p999:7.1f}us")


def run_suite(client, obj_bytes, num_ops):
    size_str = human_size(obj_bytes)
    data = bytes(random.getrandbits(8) for _ in range(obj_bytes))

    put_lats = bench(client, "put", obj_bytes, num_ops, data)
    get_lats = bench(client, "get", obj_bytes, num_ops, data)

    total_s = sum(put_lats) / 1e6
    bw_put  = (num_ops * obj_bytes / total_s) / 1e6

    total_s = sum(get_lats) / 1e6
    bw_get  = (num_ops * obj_bytes / total_s) / 1e6

    avg_put  = statistics.mean(put_lats)
    p50_put  = percentile(put_lats, 50)
    p99_put  = percentile(put_lats, 99)
    p999_put = percentile(put_lats, 99.9)

    avg_get  = statistics.mean(get_lats)
    p50_get  = percentile(get_lats, 50)
    p99_get  = percentile(get_lats, 99)
    p999_get = percentile(get_lats, 99.9)

    print(f"  PUT  {size_str:>6}  avg={avg_put:7.1f}us  p50={p50_put:7.1f}us  "
          f"p99={p99_put:7.1f}us  p999={p999_put:7.1f}us  {bw_put:.1f} MB/s")
    print(f"  GET  {size_str:>6}  avg={avg_get:7.1f}us  p50={p50_get:7.1f}us  "
          f"p99={p99_get:7.1f}us  p999={p999_get:7.1f}us  {bw_get:.1f} MB/s")


def main():
    parser = argparse.ArgumentParser(description="rdmastorage latency benchmark")
    parser.add_argument("--coord", default="node0")
    parser.add_argument("--port",  type=int, default=7777)
    parser.add_argument("-k",      type=int, default=2)
    parser.add_argument("-m",      type=int, default=1)
    parser.add_argument("--ops",   type=int, default=500, help="ops per (op, size) pair")
    parser.add_argument("--sizes", nargs="+", type=int, default=None,
                        help="object sizes in bytes (default: 256B to 1MB sweep)")
    args = parser.parse_args()

    sizes = args.sizes if args.sizes else OBJECT_SIZES

    print(f"Connecting to {args.coord}:{args.port}  k={args.k} m={args.m}")
    client = rdmastorage.Client(args.coord, args.k, args.m, args.port)
    print(f"Connected. Warmup={WARMUP_OPS} ops, bench={args.ops} ops per cell.\n")

    print(f"  {'op':<4}  {'size':>6}  {'avg':>12}  {'p50':>12}  {'p99':>12}  {'p999':>12}  {'bw':>10}")
    print("  " + "-" * 80)

    for obj_bytes in sizes:
        run_suite(client, obj_bytes, args.ops)
        print()


if __name__ == "__main__":
    main()
