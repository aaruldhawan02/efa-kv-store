#!/usr/bin/env python3
"""bench_file_latency.py — put/get latency for files of varying sizes.

For each requested file (or each size in a synthetic sweep), measures the
end-to-end put and get round-trip and emits the pybind per-phase breakdown
(encode / ctrl-RTT / RDMA / commit-RTT for PUT; decode / ctrl-RTT / RDMA
for GET). Run on a node with the pybind module (`build/rdmastorage*.so`)
present — typically the coordinator node.

Usage:
    # Synthetic size sweep (default: 1K..1M, doubling).
    python3 benchmarks/bench_file_latency.py --coord node0

    # Specific files on disk.
    python3 benchmarks/bench_file_latency.py --coord node0 \\
        --files paper.pdf image.png

    # Custom sweep, more reps for tighter percentiles.
    python3 benchmarks/bench_file_latency.py --coord node0 \\
        --sizes 4K,64K,256K,1M --reps 200

Constraints:
    Max value size = k * kMaxShardSize = k * 1 MB. For the standard
    cluster: ~2 MB at N=2/3, ~3 MB at N=4, ~4 MB at N=5/6.
"""

import argparse
import csv
import os
import statistics
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
BUILD = HERE.parent / 'build'
sys.path.insert(0, str(BUILD))

# rdmastorage is imported inside main() so --help works without the pybind
# module being built locally.

DEFAULT_SIZES = [1024, 4096, 16384, 65536, 262144, 1048576]
WARMUP = 5


def parse_size(s):
    s = s.strip().upper()
    if s.endswith('K'):
        return int(s[:-1]) * 1024
    if s.endswith('M'):
        return int(s[:-1]) * 1024 * 1024
    return int(s)


def human(n):
    for u in ('B', 'KiB', 'MiB', 'GiB'):
        if n < 1024:
            return f'{n:.0f}{u}'
        n /= 1024
    return f'{n:.0f}TiB'


def pct(sorted_data, p):
    return sorted_data[min(int(len(sorted_data) * p / 100), len(sorted_data) - 1)]


def measure(client, key_prefix, payload, reps):
    """Warm-up + reps puts + reps gets. Returns (put_lats, get_lats,
    avg_put_phases dict, avg_get_phases dict)."""
    for i in range(WARMUP):
        client.put(f'{key_prefix}-warm-{i}', payload)
        client.get(f'{key_prefix}-warm-{i}')

    put_lats = []
    put_ph = {'encode': 0.0, 'ctrl': 0.0, 'rdma': 0.0, 'commit': 0.0}
    for i in range(reps):
        key = f'{key_prefix}-{i:06d}'
        t0 = time.perf_counter()
        client.put(key, payload)
        put_lats.append((time.perf_counter() - t0) * 1e6)
        enc, ctrl, rdma, commit = client.last_put_phases()
        put_ph['encode'] += enc; put_ph['ctrl'] += ctrl
        put_ph['rdma']   += rdma; put_ph['commit'] += commit

    get_lats = []
    get_ph = {'decode': 0.0, 'ctrl': 0.0, 'rdma': 0.0}
    for i in range(reps):
        key = f'{key_prefix}-{i:06d}'
        t0 = time.perf_counter()
        out = client.get(key)
        get_lats.append((time.perf_counter() - t0) * 1e6)
        dec, ctrl, rdma = client.last_get_phases()
        get_ph['decode'] += dec; get_ph['ctrl'] += ctrl; get_ph['rdma'] += rdma
        if i == 0 and len(out) != len(payload):
            print(f'WARN: round-trip size mismatch on {key_prefix}: '
                  f'put {len(payload)}B got {len(out)}B', file=sys.stderr)

    put_ph = {k: v / reps for k, v in put_ph.items()}
    get_ph = {k: v / reps for k, v in get_ph.items()}
    return put_lats, get_lats, put_ph, get_ph


def row(label, size_bytes, reps, put_lats, get_lats, put_ph, get_ph):
    put_lats = sorted(put_lats); get_lats = sorted(get_lats)
    return {
        'label':       label,
        'size_bytes':  size_bytes,
        'reps':        reps,
        'put_avg_us':  round(statistics.mean(put_lats), 2),
        'put_p50_us':  round(pct(put_lats, 50), 2),
        'put_p95_us':  round(pct(put_lats, 95), 2),
        'put_p99_us':  round(pct(put_lats, 99), 2),
        'put_encode_us': round(put_ph['encode'], 2),
        'put_ctrl_us':   round(put_ph['ctrl'], 2),
        'put_rdma_us':   round(put_ph['rdma'], 2),
        'put_commit_us': round(put_ph['commit'], 2),
        'get_avg_us':  round(statistics.mean(get_lats), 2),
        'get_p50_us':  round(pct(get_lats, 50), 2),
        'get_p95_us':  round(pct(get_lats, 95), 2),
        'get_p99_us':  round(pct(get_lats, 99), 2),
        'get_ctrl_us':   round(get_ph['ctrl'], 2),
        'get_rdma_us':   round(get_ph['rdma'], 2),
        'get_decode_us': round(get_ph['decode'], 2),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument('--coord', default='node0')
    ap.add_argument('--port',  type=int, default=7777)
    ap.add_argument('--sizes', default=None,
                    help='comma-separated sizes (e.g. 1K,4K,1M)')
    ap.add_argument('--files', nargs='*',
                    help='specific file paths to insert and read back')
    ap.add_argument('--reps',  type=int, default=50,
                    help='measurement reps per item (default 50)')
    ap.add_argument('--csv',   default=str(HERE / 'results' / 'file_latency.csv'))
    args = ap.parse_args()

    try:
        import rdmastorage
    except ImportError as e:
        sys.exit(f'cannot import rdmastorage ({e}). Build with: make pymod')

    print(f'Connecting to {args.coord}:{args.port}...')
    client = rdmastorage.Client(args.coord, args.port)
    print(f'Connected: k={client.k} m={client.m}\n')

    rows = []
    if args.files:
        for p in args.files:
            data = Path(p).read_bytes()
            put_l, get_l, put_ph, get_ph = measure(
                client, f'file-{Path(p).name}', data, args.reps)
            rows.append(row(Path(p).name, len(data), args.reps,
                            put_l, get_l, put_ph, get_ph))
    else:
        sizes = ([parse_size(s) for s in args.sizes.split(',')]
                 if args.sizes else DEFAULT_SIZES)
        for sz in sizes:
            payload = os.urandom(sz)
            put_l, get_l, put_ph, get_ph = measure(
                client, f'sweep-{sz}', payload, args.reps)
            rows.append(row(human(sz), sz, args.reps,
                            put_l, get_l, put_ph, get_ph))

    print(f'{"item":>10}  {"reps":>5}  '
          f'{"put avg":>9} {"put p50":>9} {"put p95":>9} {"put p99":>9}  '
          f'{"get avg":>9} {"get p50":>9} {"get p95":>9} {"get p99":>9}')
    print('-' * 112)
    for r in rows:
        print(f'{r["label"]:>10}  {r["reps"]:>5}  '
              f'{r["put_avg_us"]:>9.1f} {r["put_p50_us"]:>9.1f} '
              f'{r["put_p95_us"]:>9.1f} {r["put_p99_us"]:>9.1f}  '
              f'{r["get_avg_us"]:>9.1f} {r["get_p50_us"]:>9.1f} '
              f'{r["get_p95_us"]:>9.1f} {r["get_p99_us"]:>9.1f}')

    print('\nPUT phase breakdown (avg µs):  encode | ctrl-RTT | RDMA | commit-RTT')
    for r in rows:
        print(f'  {r["label"]:>10}: enc={r["put_encode_us"]:>7.1f}  '
              f'ctrl={r["put_ctrl_us"]:>7.1f}  '
              f'rdma={r["put_rdma_us"]:>7.1f}  '
              f'commit={r["put_commit_us"]:>7.1f}')

    print('\nGET phase breakdown (avg µs):  ctrl-RTT | RDMA | decode')
    for r in rows:
        print(f'  {r["label"]:>10}: ctrl={r["get_ctrl_us"]:>7.1f}  '
              f'rdma={r["get_rdma_us"]:>7.1f}  '
              f'decode={r["get_decode_us"]:>7.1f}')

    csv_path = Path(args.csv)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f'\nWrote {csv_path}')


if __name__ == '__main__':
    main()
