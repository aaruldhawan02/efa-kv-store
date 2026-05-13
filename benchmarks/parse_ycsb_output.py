#!/usr/bin/env python3
"""parse_ycsb_output.py — convert a YCSB run-phase stdout log into one CSV row.

Reads a log file containing YCSB's [OVERALL] / [READ] / [UPDATE] / etc. blocks,
extracts throughput and the worst-case (max-across-op-types) latency stats,
and appends a single row to results/runs.csv in the shared schema:

    backend,workload,servers,k,m,threads,value_bytes,record_count,op_count,
    throughput_ops,avg_us,p95_us,p99_us,run_id,notes

Usage:
    python3 parse_ycsb_output.py results/raw/rdma_A_N6_run.log \\
        --backend rdmastorage --workload A --servers 6 --k 4 --m 2 \\
        --threads 1 --value-bytes 1024 --record-count 50000
"""

import argparse
import csv
import re
import sys
from datetime import datetime
from pathlib import Path

CSV_FIELDS = [
    'backend', 'workload', 'servers', 'k', 'm', 'threads',
    'value_bytes', 'record_count', 'op_count',
    'throughput_ops', 'avg_us', 'p95_us', 'p99_us',
    'run_id', 'notes',
]

BLOCK_RE = re.compile(r'^\[([A-Z][A-Z_]*)\],\s*([^,]+),\s*(.+?)\s*$')
SKIP_OPS = {'OVERALL', 'CLEANUP'}


def parse(log_path):
    text = Path(log_path).read_text()
    overall_thr = None
    blocks = {}
    for line in text.splitlines():
        m = BLOCK_RE.match(line)
        if not m:
            continue
        op = m.group(1)
        metric = m.group(2).strip()
        value = m.group(3).strip()
        if op == 'OVERALL':
            if metric == 'Throughput(ops/sec)':
                try:
                    overall_thr = float(value)
                except ValueError:
                    pass
            continue
        if op in SKIP_OPS or op.startswith('TOTAL'):
            continue
        blocks.setdefault(op, {})[metric] = value
    return overall_thr, blocks


def aggregate(blocks, key, agg='max'):
    vals = []
    for op, m in blocks.items():
        if key not in m:
            continue
        try:
            vals.append(float(m[key]))
        except ValueError:
            continue
    if not vals:
        return 0.0
    if agg == 'max':
        return max(vals)
    if agg == 'sum':
        return sum(vals)
    raise ValueError(f'unknown agg: {agg}')


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument('logfile')
    ap.add_argument('--backend', required=True)
    ap.add_argument('--workload', required=True)
    ap.add_argument('--servers', default='N/A')
    ap.add_argument('--k', default='N/A')
    ap.add_argument('--m', default='N/A')
    ap.add_argument('--threads', type=int, default=1)
    ap.add_argument('--value-bytes', type=int, default=1024)
    ap.add_argument('--record-count', type=int, default=50000)
    ap.add_argument('--notes', default='')
    ap.add_argument('--csv', default='benchmarks/results/runs.csv')
    args = ap.parse_args()

    thr, blocks = parse(args.logfile)
    if thr is None:
        print(f'ERROR: no [OVERALL] Throughput line in {args.logfile}',
              file=sys.stderr)
        sys.exit(1)

    op_count = int(aggregate(blocks, 'Operations', agg='sum'))
    avg_us   = aggregate(blocks, 'AverageLatency(us)',          agg='max')
    p95_us   = aggregate(blocks, '95thPercentileLatency(us)',   agg='max')
    p99_us   = aggregate(blocks, '99thPercentileLatency(us)',   agg='max')

    row = {
        'backend':        args.backend,
        'workload':       args.workload,
        'servers':        args.servers,
        'k':              args.k,
        'm':              args.m,
        'threads':        args.threads,
        'value_bytes':    args.value_bytes,
        'record_count':   args.record_count,
        'op_count':       op_count,
        'throughput_ops': round(thr, 2),
        'avg_us':         round(avg_us, 2),
        'p95_us':         round(p95_us, 2),
        'p99_us':         round(p99_us, 2),
        'run_id':         datetime.now().strftime('%Y%m%dT%H%M'),
        'notes':          args.notes,
    }

    csv_path = Path(args.csv)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not csv_path.exists()
    with open(csv_path, 'a', newline='') as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if new_file:
            w.writeheader()
        w.writerow(row)

    print(
        f'{args.backend} {args.workload} N={args.servers} '
        f'thr={thr:.0f} ops/s  avg={avg_us:.0f}us  p95={p95_us:.0f}us  '
        f'p99={p99_us:.0f}us  ops={op_count}',
        file=sys.stderr,
    )


if __name__ == '__main__':
    main()
