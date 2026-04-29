#!/usr/bin/env python3
"""
Phase breakdown benchmark for rdmastorage.
Shows how latency splits across: encode, ctrl RTT, RDMA wire, commit RTT.

Usage:
    python3 bench_phases.py [--coord node0] [-k 2] [-m 1] [--ops 200] [--size 65536]
"""

import sys
import os
import argparse
import statistics

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "build"))

try:
    import rdmastorage
except ImportError as e:
    print(f"ERROR: could not import rdmastorage: {e}")
    print("Build with: make pymod")
    sys.exit(1)

WARMUP = 20


def pct(sorted_data, p):
    return sorted_data[min(int(len(sorted_data) * p / 100), len(sorted_data) - 1)]


def bench_put_phases(client, obj_bytes, num_ops):
    data = (bytes(range(256)) * (obj_bytes // 256 + 1))[:obj_bytes]

    for i in range(WARMUP):
        client.put(f"warmup-{i}", data)

    encode_lats, ctrl_lats, rdma_lats, commit_lats, total_lats = [], [], [], [], []

    for i in range(num_ops):
        key = f"phase-{i:06d}"
        import time
        t0 = time.perf_counter()
        client.put(key, data)
        total_lats.append((time.perf_counter() - t0) * 1e6)
        enc, ctrl, rdma, commit = client.last_put_phases()
        encode_lats.append(enc)
        ctrl_lats.append(ctrl)
        rdma_lats.append(rdma)
        commit_lats.append(commit)

    print(f"\n=== PUT  n={num_ops}  obj={obj_bytes//1024}KB ===")
    print_phases([
        ("encode",     encode_lats),
        ("ctrl RTT",   ctrl_lats),
        ("RDMA write", rdma_lats),
        ("commit RTT", commit_lats),
        ("TOTAL",      total_lats),
    ])


def bench_get_phases(client, obj_bytes, num_ops):
    data = (bytes(range(256)) * (obj_bytes // 256 + 1))[:obj_bytes]

    # Pre-populate keys
    for i in range(num_ops):
        client.put(f"phase-{i:06d}", data)

    for i in range(WARMUP):
        client.get(f"phase-{i:06d}")

    decode_lats, ctrl_lats, rdma_lats, total_lats = [], [], [], []

    for i in range(num_ops):
        key = f"phase-{i:06d}"
        import time
        t0 = time.perf_counter()
        client.get(key)
        total_lats.append((time.perf_counter() - t0) * 1e6)
        dec, ctrl, rdma = client.last_get_phases()
        decode_lats.append(dec)
        ctrl_lats.append(ctrl)
        rdma_lats.append(rdma)

    print(f"\n=== GET  n={num_ops}  obj={obj_bytes//1024}KB ===")
    print_phases([
        ("decode",    decode_lats),
        ("ctrl RTT",  ctrl_lats),
        ("RDMA read", rdma_lats),
        ("TOTAL",     total_lats),
    ])


def print_phases(rows):
    header = f"  {'phase':<12}  {'avg':>8}  {'p50':>8}  {'p99':>8}  {'% total':>8}"
    print(header)
    print("  " + "-" * (len(header) - 2))

    total_avg = statistics.mean(rows[-1][1])  # TOTAL row

    for label, lats in rows:
        s = sorted(lats)
        avg = statistics.mean(s)
        p50 = pct(s, 50)
        p99 = pct(s, 99)
        pct_of_total = (avg / total_avg * 100) if total_avg > 0 else 0
        marker = " ◄" if label not in ("TOTAL",) and pct_of_total > 30 else ""
        print(f"  {label:<12}  {avg:>7.1f}us  {p50:>7.1f}us  {p99:>7.1f}us  "
              f"{pct_of_total:>7.1f}%{marker}")


def main():
    parser = argparse.ArgumentParser(description="rdmastorage phase breakdown")
    parser.add_argument("--coord", default="node0")
    parser.add_argument("--port",  type=int, default=7777)
    parser.add_argument("-k",      type=int, default=2)
    parser.add_argument("-m",      type=int, default=1)
    parser.add_argument("--ops",   type=int, default=200)
    parser.add_argument("--size",  type=int, default=65536)
    args = parser.parse_args()

    print(f"Connecting to {args.coord}:{args.port}  k={args.k} m={args.m}")
    client = rdmastorage.Client(args.coord, args.k, args.m, args.port)
    print("Connected.")

    bench_put_phases(client, args.size, args.ops)
    bench_get_phases(client, args.size, args.ops)


if __name__ == "__main__":
    main()
