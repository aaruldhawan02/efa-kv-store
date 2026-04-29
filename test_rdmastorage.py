#!/usr/bin/env python3
"""
Basic smoke test + micro-benchmark for the rdmastorage Python module.
Run on a CloudLab node after `make pymod`:

    python3 test_rdmastorage.py [--coord node0] [--k 2] [--m 1] [--port 7777]
"""

import sys
import os
import time
import argparse
import random

# Look for the built module next to this script and in build/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "build"))

try:
    import rdmastorage
except ImportError as e:
    print(f"ERROR: could not import rdmastorage: {e}")
    print("Build it first with:  make pymod")
    sys.exit(1)


def fmt_bw(bytes_total, elapsed_s):
    mb = bytes_total / elapsed_s / 1e6
    return f"{mb:.1f} MB/s"


def fmt_ops(n, elapsed_s):
    return f"{n / elapsed_s:.0f} ops/s"


# ── tests ──────────────────────────────────────────────────────────────────────

def test_put_get(client):
    print("\n[1] put/get roundtrip")
    payload = b"hello from python"
    client.put("__test_key__", payload)
    got = client.get("__test_key__")
    assert got == payload, f"mismatch: {got!r} != {payload!r}"
    print("    PASS")


def test_overwrite(client):
    print("\n[2] overwrite same key")
    client.put("__overwrite__", b"first")
    client.put("__overwrite__", b"second")
    got = client.get("__overwrite__")
    assert got == b"second", f"expected b'second', got {got!r}"
    print("    PASS")


def test_delete(client):
    print("\n[3] delete key")
    client.put("__del_key__", b"to be deleted")
    client.delete("__del_key__")
    try:
        client.get("__del_key__")
        print("    FAIL: expected exception after delete")
    except RuntimeError:
        print("    PASS")


def test_large_value(client, k):
    size = k * 64 * 1024  # 64 KiB per data shard
    print(f"\n[4] large value ({size // 1024} KiB)")
    data = bytes(random.getrandbits(8) for _ in range(size))
    client.put("__large__", data)
    got = client.get("__large__")
    assert got == data, "large value mismatch"
    print("    PASS")


def bench_put(client, num_ops, obj_bytes):
    print(f"\n[bench PUT] n={num_ops}  obj={obj_bytes}B")
    data = bytes(random.getrandbits(8) for _ in range(obj_bytes))
    latencies = []
    for i in range(num_ops):
        key = f"bench-{i:08d}"
        t0 = time.perf_counter()
        client.put(key, data)
        latencies.append(time.perf_counter() - t0)
    latencies.sort()
    n = len(latencies)
    avg = sum(latencies) / n
    p50 = latencies[int(n * 0.50)] * 1e6
    p99 = latencies[int(n * 0.99)] * 1e6
    total = sum(latencies)
    print(f"  avg={avg*1e6:.1f}us  p50={p50:.1f}us  p99={p99:.1f}us  "
          f"{fmt_ops(n, total)}  {fmt_bw(n * obj_bytes, total)}")


def bench_get(client, num_ops, obj_bytes):
    print(f"\n[bench GET] n={num_ops}  obj={obj_bytes}B")
    latencies = []
    for i in range(num_ops):
        key = f"bench-{i:08d}"
        t0 = time.perf_counter()
        got = client.get(key)
        latencies.append(time.perf_counter() - t0)
        assert len(got) == obj_bytes
    latencies.sort()
    n = len(latencies)
    avg = sum(latencies) / n
    p50 = latencies[int(n * 0.50)] * 1e6
    p99 = latencies[int(n * 0.99)] * 1e6
    total = sum(latencies)
    print(f"  avg={avg*1e6:.1f}us  p50={p50:.1f}us  p99={p99:.1f}us  "
          f"{fmt_ops(n, total)}  {fmt_bw(n * obj_bytes, total)}")


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="rdmastorage smoke test + bench")
    parser.add_argument("--coord", default="node0", help="coordinator hostname")
    parser.add_argument("--port",  type=int, default=7777)
    parser.add_argument("--ops",   type=int, default=500,   help="ops per bench")
    parser.add_argument("--size",  type=int, default=65536, help="object bytes for bench")
    parser.add_argument("--no-bench", action="store_true", help="skip benchmarks")
    args = parser.parse_args()

    print(f"Connecting to coordinator {args.coord}:{args.port}")
    client = rdmastorage.Client(args.coord, args.port)
    print(f"Connected. k={client.k} m={client.m}")

    test_put_get(client)
    test_overwrite(client)
    test_delete(client)
    test_large_value(client, client.k)

    if not args.no_bench:
        bench_put(client, args.ops, args.size)
        bench_get(client, args.ops, args.size)

    print("\nAll tests passed.")


if __name__ == "__main__":
    main()
