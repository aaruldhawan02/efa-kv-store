#!/usr/bin/env python3
"""
bench_ycsb.py — YCSB-style workload benchmarks for RDMA-Distributed-KV-Store.

Workloads (matching YCSB definitions):
    A — 50% read / 50% update   (heavy update)
    B — 95% read /  5% update   (mostly reads)
    C — 100% read               (read only)
    D — 95% read /  5% insert   (read latest; newest keys are hottest)

Key distribution:
    uniform  — each key equally likely
    zipfian  — skewed: ~20% of keys receive ~80% of requests (default)

Usage:
    python3 bench_ycsb.py --coord node0 --workload A
    python3 bench_ycsb.py --coord node0 --workload B --ops 1000 --size 65536
    python3 bench_ycsb.py --coord node0 --workload C --dist uniform --no-phases
"""

import sys, os, time, argparse, random, statistics, math
sys.path.insert(0, os.path.expanduser('~/efa-kv-store/build'))
import rdmastorage

# ---------------------------------------------------------------------------
# YCSB workload definitions: (read_frac, update_frac, insert_frac)
# ---------------------------------------------------------------------------

YCSB_WORKLOADS = {
    'A': (0.50, 0.50, 0.00),
    'B': (0.95, 0.05, 0.00),
    'C': (1.00, 0.00, 0.00),
    'D': (0.95, 0.00, 0.05),
}

WORKLOAD_DESC = {
    'A': '50% read / 50% update  — heavy update',
    'B': '95% read /  5% update  — mostly reads',
    'C': '100% read              — read only',
    'D': '95% read /  5% insert  — read latest',
}

DEFAULT_SIZE   = 64 * 1024   # 64 KB
DEFAULT_OPS    = 500
DEFAULT_WARMUP = 50
DEFAULT_KEYS   = 1000        # key-space size
COORD          = 'node0'

# ---------------------------------------------------------------------------
# Zipfian sampler
# ---------------------------------------------------------------------------

class ZipfianSampler:
    """Approximates the YCSB Zipfian distribution over [0, n)."""
    def __init__(self, n, theta=0.99):
        self.n = n
        self.theta = theta
        # Zeta normalization
        self.zeta_n = sum(1.0 / (i ** theta) for i in range(1, n + 1))

    def sample(self):
        u = random.random()
        cumulative = 0.0
        for i in range(1, self.n + 1):
            cumulative += (1.0 / (i ** self.theta)) / self.zeta_n
            if u <= cumulative:
                return i - 1
        return self.n - 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_data(size):
    return (bytes(range(256)) * (size // 256 + 1))[:size]

def key_name(i):
    return f'ycsb-{i:06d}'

def stats(lats_us):
    lats_us = sorted(lats_us)
    n = len(lats_us)
    return {
        'avg': statistics.mean(lats_us),
        'p50': lats_us[int(n * 0.50)],
        'p95': lats_us[int(n * 0.95)],
        'p99': lats_us[int(n * 0.99)],
    }

def fmt_size(b):
    if b >= 1024 * 1024: return f'{b // (1024*1024)}MB'
    if b >= 1024:        return f'{b // 1024}KB'
    return f'{b}B'

def print_row(op, s):
    print(f'  {op:<8}  '
          f'avg={s["avg"]:8.1f}us  '
          f'p50={s["p50"]:8.1f}us  '
          f'p95={s["p95"]:8.1f}us  '
          f'p99={s["p99"]:8.1f}us')

def print_put_phases(client):
    enc, ctrl, rdma, cmmt = client.last_put_phases()
    total = enc + ctrl + rdma + cmmt
    rows = [('encode',     enc),
            ('ctrl RTT',   ctrl),
            ('RDMA write', rdma),
            ('commit RTT', cmmt)]
    for label, val in rows:
        pct = val / total * 100 if total > 0 else 0
        print(f'           {label:<12} {val:7.1f}us  ({pct:.0f}%)')

def print_get_phases(client):
    dec, ctrl, rdma = client.last_get_phases()
    total = dec + ctrl + rdma
    rows = [('ctrl RTT',  ctrl),
            ('RDMA read', rdma),
            ('decode',    dec)]
    for label, val in rows:
        pct = val / total * 100 if total > 0 else 0
        print(f'           {label:<12} {val:7.1f}us  ({pct:.0f}%)')

# ---------------------------------------------------------------------------
# Core runner
# ---------------------------------------------------------------------------

def run_ycsb(client, workload, size, num_ops, warmup, num_keys,
             dist, show_phases):
    read_frac, update_frac, insert_frac = YCSB_WORKLOADS[workload]
    data  = make_data(size)
    sampler = ZipfianSampler(num_keys) if dist == 'zipfian' else None

    def pick_key(existing_keys):
        if sampler:
            return existing_keys[sampler.sample() % len(existing_keys)]
        return random.choice(existing_keys)

    # Populate key-space so reads have something to hit.
    print(f'  Populating {num_keys} keys ({fmt_size(size)} each)...', end='', flush=True)
    existing_keys = [key_name(i) for i in range(num_keys)]
    for k in existing_keys:
        client.put(k, data)
    print(' done.')

    # Warmup
    for _ in range(warmup):
        client.get(pick_key(existing_keys))

    # Workload D: track insertion counter for "read latest" behaviour
    insert_counter = num_keys

    put_lats, get_lats = [], []
    op_counts = {'read': 0, 'update': 0, 'insert': 0}

    for _ in range(num_ops):
        roll = random.random()

        if roll < read_frac:
            # READ
            k = pick_key(existing_keys)
            t0 = time.perf_counter()
            client.get(k)
            get_lats.append((time.perf_counter() - t0) * 1e6)
            op_counts['read'] += 1
            if show_phases and len(get_lats) == 1:
                pass  # phases printed at summary

        elif roll < read_frac + update_frac:
            # UPDATE (overwrite existing key)
            k = pick_key(existing_keys)
            t0 = time.perf_counter()
            client.put(k, data)
            put_lats.append((time.perf_counter() - t0) * 1e6)
            op_counts['update'] += 1

        else:
            # INSERT (new key — workload D)
            k = key_name(insert_counter)
            insert_counter += 1
            existing_keys.append(k)
            t0 = time.perf_counter()
            client.put(k, data)
            put_lats.append((time.perf_counter() - t0) * 1e6)
            op_counts['insert'] += 1

    # Results
    print(f'\n  Operations: read={op_counts["read"]}  '
          f'update={op_counts["update"]}  insert={op_counts["insert"]}')
    print(f'  {"op":<8}  {"avg":>10}    {"p50":>10}    {"p95":>10}    {"p99":>10}')
    print(f'  ' + '-' * 60)

    if get_lats:
        print_row('READ', stats(get_lats))
        if show_phases and get_lats:
            # Re-run one GET to capture phases for display
            client.get(pick_key(existing_keys))
            print_get_phases(client)

    if put_lats:
        label = 'UPDATE' if op_counts['insert'] == 0 else 'PUT'
        print_row(label, stats(put_lats))
        if show_phases and put_lats:
            client.put(pick_key(existing_keys), data)
            print_put_phases(client)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='YCSB-style benchmark for RDMA-Distributed-KV-Store')
    parser.add_argument('--coord',     default=COORD,
                        help='coordinator hostname (default: node0)')
    parser.add_argument('--port',      type=int, default=7777)
    parser.add_argument('--workload',  default='A', choices=list(YCSB_WORKLOADS),
                        help='YCSB workload A/B/C/D (default: A)')
    parser.add_argument('--size',      type=int, default=DEFAULT_SIZE,
                        metavar='BYTES',
                        help=f'object size in bytes (default: {DEFAULT_SIZE})')
    parser.add_argument('--ops',       type=int, default=DEFAULT_OPS,
                        help=f'number of measured operations (default: {DEFAULT_OPS})')
    parser.add_argument('--warmup',    type=int, default=DEFAULT_WARMUP,
                        help=f'warmup operations (default: {DEFAULT_WARMUP})')
    parser.add_argument('--keys',      type=int, default=DEFAULT_KEYS,
                        help=f'key-space size (default: {DEFAULT_KEYS})')
    parser.add_argument('--dist',      default='zipfian', choices=['zipfian', 'uniform'],
                        help='key selection distribution (default: zipfian)')
    parser.add_argument('--no-phases', action='store_true',
                        help='skip per-phase breakdown')
    args = parser.parse_args()

    print(f'Connecting to coordinator at {args.coord}:{args.port}...')
    client = rdmastorage.Client(args.coord, args.port)
    print(f'Connected  : k={client.k}  m={client.m}')
    print(f'Workload   : YCSB-{args.workload}  ({WORKLOAD_DESC[args.workload]})')
    print(f'Object size: {fmt_size(args.size)}')
    print(f'Ops        : {args.ops}  (warmup: {args.warmup})')
    print(f'Key space  : {args.keys}  distribution: {args.dist}')
    print()

    run_ycsb(client, args.workload, args.size, args.ops, args.warmup,
             args.keys, args.dist, not args.no_phases)


if __name__ == '__main__':
    main()
