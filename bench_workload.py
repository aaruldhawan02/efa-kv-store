#!/usr/bin/env python3
"""
bench_workload.py — configurable workload benchmarks for efa-kv-store.

Usage:
    python3 bench_workload.py --coord node0 --workload put_get_delete
    python3 bench_workload.py --coord node0 --workload put_get --sizes 4096 65536 --ops 500
    python3 bench_workload.py --coord node0 --workload put_only --sizes 65536 --ops 1000
    python3 bench_workload.py --coord node0 --workload put_get --no-phases

Workloads:
    put_only         — PUT only
    get_only         — PUT to populate, then GET only
    put_get          — alternating PUT then GET
    put_get_delete   — PUT, GET, DELETE cycle
"""

import sys, os, time, argparse, statistics
sys.path.insert(0, os.path.expanduser('~/efa-kv-store/build'))
import rdmastorage

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DEFAULT_SIZES  = [4 * 1024, 16 * 1024, 64 * 1024, 256 * 1024]  # bytes
DEFAULT_OPS    = 200
DEFAULT_WARMUP = 20
COORD          = 'node0'

WORKLOADS = ['put_only', 'get_only', 'put_get', 'put_get_delete']

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_data(size):
    return (bytes(range(256)) * (size // 256 + 1))[:size]

def make_keys(n, prefix='bench'):
    return [f'{prefix}-{i:06d}' for i in range(n)]

def stats(lats_us):
    lats_us = sorted(lats_us)
    n = len(lats_us)
    return {
        'avg': statistics.mean(lats_us),
        'p50': lats_us[int(n * 0.50)],
        'p95': lats_us[int(n * 0.95)],
        'p99': lats_us[int(n * 0.99)],
    }

def phase_stats(phase_lists):
    return {name: stats(vals) for name, vals in phase_lists.items()}

def fmt_size(b):
    if b >= 1024 * 1024: return f'{b // (1024*1024)}MB'
    if b >= 1024:        return f'{b // 1024}KB'
    return f'{b}B'

def bw(size, avg_us):
    return size / (avg_us / 1e6) / 1e6

def print_latency_row(op, size, s):
    print(f'  {op:<6} {fmt_size(size):>6}  '
          f'avg={s["avg"]:8.1f}us  '
          f'p50={s["p50"]:8.1f}us  '
          f'p95={s["p95"]:8.1f}us  '
          f'p99={s["p99"]:8.1f}us  '
          f'{bw(size, s["avg"]):6.1f} MB/s')

def print_put_phases(phases, size):
    enc  = phases['encode']
    ctrl = phases['ctrl_rtt']
    rdma = phases['rdma']
    cmmt = phases['commit_rtt']
    tot  = stats([e+c+r+k for e,c,r,k in zip(
        sorted(phases['encode']), sorted(phases['ctrl_rtt']),
        sorted(phases['rdma']),   sorted(phases['commit_rtt']))])
    print(f'         {"phase":<12}  {"avg":>8}   {"p50":>8}   {"p95":>8}   {"p99":>8}')
    print(f'         {"-"*56}')
    for label, vals in [('encode', enc), ('ctrl RTT', ctrl),
                        ('RDMA write', rdma), ('commit RTT', cmmt)]:
        s = stats(vals)
        pct = s['avg'] / tot['avg'] * 100
        print(f'         {label:<12}  {s["avg"]:>7.1f}us  {s["p50"]:>7.1f}us  '
              f'{s["p95"]:>7.1f}us  {s["p99"]:>7.1f}us  ({pct:.0f}%)')
    print(f'         {"total":<12}  {tot["avg"]:>7.1f}us  {tot["p50"]:>7.1f}us  '
          f'{tot["p95"]:>7.1f}us  {tot["p99"]:>7.1f}us')

def print_get_phases(phases, size):
    ctrl = phases['ctrl_rtt']
    rdma = phases['rdma']
    dec  = phases['decode']
    tot  = stats([c+r+d for c,r,d in zip(
        sorted(phases['ctrl_rtt']), sorted(phases['rdma']),
        sorted(phases['decode']))])
    print(f'         {"phase":<12}  {"avg":>8}   {"p50":>8}   {"p95":>8}   {"p99":>8}')
    print(f'         {"-"*56}')
    for label, vals in [('ctrl RTT', ctrl), ('RDMA read', rdma), ('decode', dec)]:
        s = stats(vals)
        pct = s['avg'] / tot['avg'] * 100
        print(f'         {label:<12}  {s["avg"]:>7.1f}us  {s["p50"]:>7.1f}us  '
              f'{s["p95"]:>7.1f}us  {s["p99"]:>7.1f}us  ({pct:.0f}%)')
    print(f'         {"total":<12}  {tot["avg"]:>7.1f}us  {tot["p50"]:>7.1f}us  '
          f'{tot["p95"]:>7.1f}us  {tot["p99"]:>7.1f}us')

# ---------------------------------------------------------------------------
# Workload runners
# ---------------------------------------------------------------------------

def run_put_only(client, size, num_ops, warmup, show_phases):
    data = make_data(size)
    keys = make_keys(num_ops)
    for k in keys[:warmup]:
        client.put(k, data)

    lats = []
    put_phases = {'encode': [], 'ctrl_rtt': [], 'rdma': [], 'commit_rtt': []}
    for k in keys:
        t0 = time.perf_counter()
        client.put(k, data)
        lats.append((time.perf_counter() - t0) * 1e6)
        if show_phases:
            enc, ctrl, rdma, cmmt = client.last_put_phases()
            put_phases['encode'].append(enc)
            put_phases['ctrl_rtt'].append(ctrl)
            put_phases['rdma'].append(rdma)
            put_phases['commit_rtt'].append(cmmt)

    s = stats(lats)
    print_latency_row('PUT', size, s)
    if show_phases:
        print_put_phases(put_phases, size)


def run_get_only(client, size, num_ops, warmup, show_phases):
    data = make_data(size)
    keys = make_keys(num_ops)
    for k in keys:
        client.put(k, data)
    for k in keys[:warmup]:
        client.get(k)

    lats = []
    get_phases = {'ctrl_rtt': [], 'rdma': [], 'decode': []}
    for k in keys:
        t0 = time.perf_counter()
        client.get(k)
        lats.append((time.perf_counter() - t0) * 1e6)
        if show_phases:
            dec, ctrl, rdma = client.last_get_phases()
            get_phases['ctrl_rtt'].append(ctrl)
            get_phases['rdma'].append(rdma)
            get_phases['decode'].append(dec)

    s = stats(lats)
    print_latency_row('GET', size, s)
    if show_phases:
        print_get_phases(get_phases, size)


def run_put_get(client, size, num_ops, warmup, show_phases):
    data = make_data(size)
    keys = make_keys(num_ops)
    for k in keys[:warmup]:
        client.put(k, data)
        client.get(k)

    put_lats, get_lats = [], []
    put_phases = {'encode': [], 'ctrl_rtt': [], 'rdma': [], 'commit_rtt': []}
    get_phases = {'ctrl_rtt': [], 'rdma': [], 'decode': []}
    for k in keys:
        t0 = time.perf_counter()
        client.put(k, data)
        put_lats.append((time.perf_counter() - t0) * 1e6)
        if show_phases:
            enc, ctrl, rdma, cmmt = client.last_put_phases()
            put_phases['encode'].append(enc)
            put_phases['ctrl_rtt'].append(ctrl)
            put_phases['rdma'].append(rdma)
            put_phases['commit_rtt'].append(cmmt)

        t0 = time.perf_counter()
        client.get(k)
        get_lats.append((time.perf_counter() - t0) * 1e6)
        if show_phases:
            dec, ctrl, rdma = client.last_get_phases()
            get_phases['ctrl_rtt'].append(ctrl)
            get_phases['rdma'].append(rdma)
            get_phases['decode'].append(dec)

    sp = stats(put_lats)
    sg = stats(get_lats)
    print_latency_row('PUT', size, sp)
    if show_phases:
        print_put_phases(put_phases, size)
    print_latency_row('GET', size, sg)
    if show_phases:
        print_get_phases(get_phases, size)


def run_put_get_delete(client, size, num_ops, warmup, show_phases):
    data = make_data(size)
    keys = make_keys(num_ops)
    for k in keys[:warmup]:
        client.put(k, data)
        client.get(k)
        client.delete(k)

    put_lats, get_lats, del_lats = [], [], []
    put_phases = {'encode': [], 'ctrl_rtt': [], 'rdma': [], 'commit_rtt': []}
    get_phases = {'ctrl_rtt': [], 'rdma': [], 'decode': []}
    for k in keys:
        t0 = time.perf_counter()
        client.put(k, data)
        put_lats.append((time.perf_counter() - t0) * 1e6)
        if show_phases:
            enc, ctrl, rdma, cmmt = client.last_put_phases()
            put_phases['encode'].append(enc)
            put_phases['ctrl_rtt'].append(ctrl)
            put_phases['rdma'].append(rdma)
            put_phases['commit_rtt'].append(cmmt)

        t0 = time.perf_counter()
        client.get(k)
        get_lats.append((time.perf_counter() - t0) * 1e6)
        if show_phases:
            dec, ctrl, rdma = client.last_get_phases()
            get_phases['ctrl_rtt'].append(ctrl)
            get_phases['rdma'].append(rdma)
            get_phases['decode'].append(dec)

        t0 = time.perf_counter()
        client.delete(k)
        del_lats.append((time.perf_counter() - t0) * 1e6)

    sp = stats(put_lats)
    sg = stats(get_lats)
    sd = stats(del_lats)
    print_latency_row('PUT', size, sp)
    if show_phases:
        print_put_phases(put_phases, size)
    print_latency_row('GET', size, sg)
    if show_phases:
        print_get_phases(get_phases, size)
    print_latency_row('DELETE', size, sd)


RUNNERS = {
    'put_only':       run_put_only,
    'get_only':       run_get_only,
    'put_get':        run_put_get,
    'put_get_delete': run_put_get_delete,
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='efa-kv-store workload benchmark')
    parser.add_argument('--coord',      default=COORD,
                        help='coordinator hostname (default: node0)')
    parser.add_argument('--port',       type=int, default=7777)
    parser.add_argument('--workload',   default='put_get',
                        choices=WORKLOADS,
                        help='workload type (default: put_get)')
    parser.add_argument('--sizes',      type=int, nargs='+', default=DEFAULT_SIZES,
                        metavar='BYTES',
                        help='object sizes in bytes (default: 4096 16384 65536 262144)')
    parser.add_argument('--ops',        type=int, default=DEFAULT_OPS,
                        help=f'ops per size (default: {DEFAULT_OPS})')
    parser.add_argument('--warmup',     type=int, default=DEFAULT_WARMUP,
                        help=f'warmup ops per size (default: {DEFAULT_WARMUP})')
    parser.add_argument('--no-phases',  action='store_true',
                        help='skip per-phase breakdown (faster output)')
    args = parser.parse_args()
    show_phases = not args.no_phases

    print(f'Connecting to coordinator at {args.coord}:{args.port}...')
    client = rdmastorage.Client(args.coord, args.port)
    print(f'Connected : k={client.k}  m={client.m}  (tolerates {client.m} node failures)')
    print(f'Workload  : {args.workload}')
    print(f'Ops/size  : {args.ops}  (warmup: {args.warmup})')
    print(f'Phases    : {"yes" if show_phases else "no"}')
    print()

    lat_header = (f'  {"op":<6} {"size":>6}  '
                  f'{"avg":>10}    {"p50":>10}    {"p95":>10}    {"p99":>10}    {"bw":>8}')
    sep = '  ' + '-' * (len(lat_header) - 2)
    runner = RUNNERS[args.workload]

    for size in args.sizes:
        print(f'--- {fmt_size(size)} ---')
        print(lat_header)
        print(sep)
        try:
            runner(client, size, args.ops, args.warmup, show_phases)
        except Exception as e:
            print(f'  ERROR: {e}')
        print()


if __name__ == '__main__':
    main()
