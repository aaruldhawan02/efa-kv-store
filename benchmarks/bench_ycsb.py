#!/usr/bin/env python3
"""bench_ycsb.py — YCSB-style benchmark driver for RDMAStorage.

Parses standard Java YCSB workload property files and runs the load
or run phase against rdmastorage.Client. No Java involved.

Usage:
    # Load phase: insert N records
    python3 benchmarks/bench_ycsb.py --coord node0 --phase load \
        --workload benchmarks/workloads/workloada --records 100000 --value-bytes 1024

    # Run phase: do M ops per the workload's read/update/insert/rmw proportions
    python3 benchmarks/bench_ycsb.py --coord node0 --phase run \
        --workload benchmarks/workloads/workloada \
        --records 100000 --ops 10000 --value-bytes 1024 \
        --output benchmarks/results/rdma_A_N3.json

Refuses to run if the workload has scanproportion > 0 — RDMAStorage has no scan.
"""

import argparse
import csv
import json
import random
import sys
import time
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
BUILD = HERE.parent / 'build'
sys.path.insert(0, str(BUILD))

import rdmastorage


# ---------------------------------------------------------------------------
# Workload file parsing
# ---------------------------------------------------------------------------

def parse_workload(path):
    props = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            props[k.strip()] = v.strip()
    return props


# ---------------------------------------------------------------------------
# Key / value generation
# ---------------------------------------------------------------------------

KEY_WIDTH = 10

def make_key(i):
    return f'user{i:0{KEY_WIDTH}d}'


# ---------------------------------------------------------------------------
# Distributions
# ---------------------------------------------------------------------------

class UniformGen:
    def __init__(self, n, rng):
        self.n, self.rng = n, rng

    def next(self):
        return self.rng.randrange(self.n)


class ZipfianGen:
    """YCSB ZipfianGenerator over [0, n-1], theta=0.99 by default."""
    def __init__(self, n, rng, theta=0.99):
        self.n, self.theta, self.rng = n, theta, rng
        self.zeta_n = sum(1.0 / (i ** theta) for i in range(1, n + 1))
        self.zeta_2 = 1.0 + 1.0 / (2 ** theta)
        self.alpha = 1.0 / (1.0 - theta)
        self.eta = (
            (1.0 - (2.0 / n) ** (1.0 - theta))
            / (1.0 - self.zeta_2 / self.zeta_n)
        )

    def next(self):
        u = self.rng.random()
        uz = u * self.zeta_n
        if uz < 1.0:
            return 0
        if uz < 1.0 + 0.5 ** self.theta:
            return 1
        idx = int(self.n * ((self.eta * u - self.eta + 1.0) ** self.alpha))
        return min(max(idx, 0), self.n - 1)


class LatestGen:
    """YCSB-style latest: most-recently-inserted are most popular."""
    def __init__(self, n, rng, theta=0.99):
        self.n = n
        self.inner = ZipfianGen(n, rng, theta)

    def next(self):
        return self.n - 1 - self.inner.next()


def build_distribution(name, n, rng):
    name = name.lower()
    if name == 'uniform':
        return UniformGen(n, rng)
    if name == 'zipfian':
        return ZipfianGen(n, rng)
    if name == 'latest':
        return LatestGen(n, rng)
    raise ValueError(f'unknown distribution: {name}')


# ---------------------------------------------------------------------------
# Op selection
# ---------------------------------------------------------------------------

def build_op_cdf(props):
    weights = [
        ('read',            float(props.get('readproportion', 0))),
        ('update',          float(props.get('updateproportion', 0))),
        ('insert',          float(props.get('insertproportion', 0))),
        ('scan',            float(props.get('scanproportion', 0))),
        ('readmodifywrite', float(props.get('readmodifywriteproportion', 0))),
    ]
    if any(op == 'scan' and w > 0 for op, w in weights):
        raise SystemExit(
            'scanproportion > 0: RDMAStorage has no scan support, refusing to run'
        )
    total = sum(w for _, w in weights)
    if total <= 0:
        raise SystemExit('all op proportions are zero')
    cdf, cum = [], 0.0
    for op, w in weights:
        if w > 0:
            cum += w / total
            cdf.append((cum, op))
    return cdf


def pick_op(rng, cdf):
    r = rng.random()
    for cum, op in cdf:
        if r < cum:
            return op
    return cdf[-1][1]


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def percentiles(lats, ps=(0.50, 0.95, 0.99)):
    if not lats:
        return {f'p{int(p*100)}': 0.0 for p in ps}
    s = sorted(lats)
    return {
        f'p{int(p*100)}': s[min(int(len(s) * p), len(s) - 1)]
        for p in ps
    }


# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------

def phase_load(client, records, value):
    t0 = time.perf_counter()
    for i in range(records):
        client.put(make_key(i), value)
    return time.perf_counter() - t0


def phase_run(client, props, records, ops, value, rng, warmup_frac):
    cdf = build_op_cdf(props)
    dist_name = props.get('requestdistribution', 'uniform')
    keygen = build_distribution(dist_name, records, rng)
    inserted = records
    n_warmup = int(ops * warmup_frac)

    op_lats = {'read': [], 'update': [], 'insert': [], 'readmodifywrite': []}

    t_start = time.perf_counter()
    for i in range(ops):
        op = pick_op(rng, cdf)
        if op == 'read':
            k = make_key(keygen.next())
            t0 = time.perf_counter()
            client.get(k)
        elif op == 'update':
            k = make_key(keygen.next())
            t0 = time.perf_counter()
            client.put(k, value)
        elif op == 'insert':
            k = make_key(inserted)
            inserted += 1
            t0 = time.perf_counter()
            client.put(k, value)
        elif op == 'readmodifywrite':
            k = make_key(keygen.next())
            t0 = time.perf_counter()
            client.get(k)
            client.put(k, value)
        else:
            raise RuntimeError(f'unexpected op: {op}')

        lat_us = (time.perf_counter() - t0) * 1e6
        if i >= n_warmup:
            op_lats[op].append(lat_us)

    elapsed = time.perf_counter() - t_start
    return elapsed, op_lats, ops - n_warmup


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

CSV_FIELDS = [
    'backend', 'workload', 'servers', 'k', 'm', 'threads',
    'value_bytes', 'record_count', 'op_count',
    'throughput_ops', 'p50_us', 'p95_us', 'p99_us',
    'run_id', 'notes',
]

def append_csv(path, row):
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists()
    with open(path, 'a', newline='') as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if new_file:
            w.writeheader()
        w.writerow(row)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument('--coord', default='node0')
    ap.add_argument('--port', type=int, default=7777)
    ap.add_argument('--phase', choices=['load', 'run'], required=True)
    ap.add_argument('--workload', required=True, help='YCSB property file path')
    ap.add_argument('--records', type=int, default=None,
                    help='override recordcount from the workload file')
    ap.add_argument('--ops', type=int, default=None,
                    help='override operationcount from the workload file')
    ap.add_argument('--value-bytes', type=int, default=1024)
    ap.add_argument('--seed', type=int, default=42)
    ap.add_argument('--warmup-frac', type=float, default=0.1,
                    help='fraction of run-phase ops to drop from stats')
    ap.add_argument('--output', default=None, help='summary JSON output path')
    ap.add_argument('--csv', default=str(HERE / 'results' / 'runs.csv'),
                    help='shared results CSV to append to')
    ap.add_argument('--notes', default='')
    args = ap.parse_args()

    props = parse_workload(args.workload)
    records = args.records or int(props.get('recordcount', 1000))
    ops     = args.ops     or int(props.get('operationcount', 1000))
    workload_name = (
        Path(args.workload).name.replace('workload', '').upper() or 'X'
    )

    rng = random.Random(args.seed)
    value = rng.randbytes(args.value_bytes)

    print(f'Connecting to coordinator at {args.coord}:{args.port}...')
    client = rdmastorage.Client(args.coord, args.port)
    print(f'Connected: k={client.k}  m={client.m}')
    print(f'Workload : {workload_name}  ({args.workload})')
    print(f'Phase    : {args.phase}')
    print(f'Records  : {records}    Ops: {ops}    Value: {args.value_bytes}B')
    print()

    if args.phase == 'load':
        elapsed = phase_load(client, records, value)
        thr = records / elapsed
        print(f'Load done: {records} records in {elapsed:.2f}s  ({thr:.0f} ops/s)')
        return

    elapsed, op_lats, measured = phase_run(
        client, props, records, ops, value, rng, args.warmup_frac
    )
    all_lats = [x for lst in op_lats.values() for x in lst]
    pcts = percentiles(all_lats)
    thr  = measured / elapsed

    print(f'Run done: {measured} measured ops in {elapsed:.2f}s  ({thr:.0f} ops/s)')
    print(f'  overall  p50={pcts["p50"]:7.1f}us  p95={pcts["p95"]:7.1f}us  p99={pcts["p99"]:7.1f}us')
    for op, lats in op_lats.items():
        if not lats:
            continue
        p = percentiles(lats)
        print(f'  {op:<16} n={len(lats):<6}  '
              f'p50={p["p50"]:7.1f}us  p95={p["p95"]:7.1f}us  p99={p["p99"]:7.1f}us')

    run_id = datetime.now().strftime('%Y%m%dT%H%M')
    summary = {
        'backend': 'rdmastorage',
        'workload': workload_name,
        'servers': client.k + client.m,
        'k': client.k,
        'm': client.m,
        'threads': 1,
        'value_bytes': args.value_bytes,
        'record_count': records,
        'op_count': measured,
        'throughput_ops': round(thr, 2),
        'p50_us': round(pcts['p50'], 2),
        'p95_us': round(pcts['p95'], 2),
        'p99_us': round(pcts['p99'], 2),
        'run_id': run_id,
        'notes': args.notes,
    }

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(summary, indent=2))
        print(f'Wrote {out}')

    append_csv(Path(args.csv), summary)
    print(f'Appended row to {args.csv}')


if __name__ == '__main__':
    main()
