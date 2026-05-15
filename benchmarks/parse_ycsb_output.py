#!/usr/bin/env python3
"""parse_ycsb_output.py — turn YCSB run-phase logs into rows in runs.csv.

Two modes:

  1. Single log (append one row):
       python3 benchmarks/parse_ycsb_output.py benchmarks/results/raw/rdma_A_N6_run.log

  2. Scan a directory (rebuild runs.csv from every *_run.log it finds):
       python3 benchmarks/parse_ycsb_output.py --scan benchmarks/results/raw

Filename convention:
    rdma_<workload>_N<N>_run.log       → rdmastorage, servers=N
    s3_<workload>_run.log              → s3express,  servers=N/A
Workload letter is taken from the filename. For rdmastorage rows, (k, m) come
from the coordinator's canonical policy table (1→(1,0), 2→(2,0), 3→(2,1),
4→(3,1), 5→(4,1), 6→(4,2)).

Any inferred field can be overridden via CLI flags.

CSV schema:
    backend,workload,servers,k,m,threads,value_bytes,record_count,op_count,
    throughput_ops,avg_us,p95_us,p99_us,run_id,notes
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
    'throughput_ops', 'avg_us', 'p50_us', 'p90_us', 'p95_us', 'p99_us',
    'run_id', 'notes',
]

BLOCK_RE = re.compile(r'^\[([A-Z][A-Z_]*)\],\s*([^,]+),\s*(.+?)\s*$')
FNAME_RE = re.compile(
    r'(?P<prefix>[A-Za-z0-9]+)_(?P<workload>[A-Za-z])'
    r'(?:_N(?P<servers>\d+))?'
    r'(?:_(?P<threads>\d+)t)?'
    r'_run\.log$'
)

PREFIX_TO_BACKEND = {
    'rdma':        'rdmastorage',
    'rdmastorage': 'rdmastorage',
    's3':          's3express',
    's3express':   's3express',
}

# Coordinator's (k, m) policy as a function of live server count.
# N=1 omitted: the coordinator requires >=2 servers before clients can connect.
K_M_BY_N = {2: (2, 0), 3: (2, 1), 4: (3, 1), 5: (4, 1), 6: (4, 2)}

SKIP_OPS = {'OVERALL', 'CLEANUP'}


def parse_log(log_path):
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
    for m in blocks.values():
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


def infer_from_filename(name):
    m = FNAME_RE.search(name)
    if not m:
        return {}
    prefix = m.group('prefix').lower()
    out = {
        'backend':  PREFIX_TO_BACKEND.get(prefix, prefix),
        'workload': m.group('workload').upper(),
    }
    if m.group('servers'):
        out['servers'] = int(m.group('servers'))
    if m.group('threads'):
        out['threads'] = int(m.group('threads'))
    return out


def build_row(log_path, overrides):
    name = Path(log_path).name
    fields = infer_from_filename(name)
    fields.update({k: v for k, v in overrides.items() if v is not None})

    backend  = fields.get('backend', 'rdmastorage')
    workload = fields.get('workload', 'X')
    servers  = fields.get('servers')
    k        = fields.get('k')
    m_val    = fields.get('m')
    threads  = fields.get('threads', 1)
    value_bytes  = fields.get('value_bytes', 1024)
    record_count = fields.get('record_count', 50000)
    notes    = fields.get('notes', '')

    if backend == 'rdmastorage' and isinstance(servers, int):
        default_k, default_m = K_M_BY_N.get(servers, (None, None))
        servers_out = servers
        k_out = k if k is not None else (default_k if default_k is not None else 'N/A')
        m_out = m_val if m_val is not None else (default_m if default_m is not None else 'N/A')
    elif backend == 'rdmastorage':
        servers_out = 'N/A' if servers is None else servers
        k_out = 'N/A' if k is None else k
        m_out = 'N/A' if m_val is None else m_val
    else:
        servers_out = 'N/A'
        k_out = 'N/A'
        m_out = 'N/A'

    thr, blocks = parse_log(log_path)
    if thr is None:
        raise RuntimeError(f'no [OVERALL] Throughput line in {log_path}')

    op_count = int(aggregate(blocks, 'Operations', agg='sum'))
    avg_us   = aggregate(blocks, 'AverageLatency(us)',          agg='max')
    p50_us   = aggregate(blocks, '50thPercentileLatency(us)',   agg='max')
    p90_us   = aggregate(blocks, '90thPercentileLatency(us)',   agg='max')
    p95_us   = aggregate(blocks, '95thPercentileLatency(us)',   agg='max')
    p99_us   = aggregate(blocks, '99thPercentileLatency(us)',   agg='max')

    return {
        'backend':        backend,
        'workload':       workload,
        'servers':        servers_out,
        'k':              k_out,
        'm':              m_out,
        'threads':        threads,
        'value_bytes':    value_bytes,
        'record_count':   record_count,
        'op_count':       op_count,
        'throughput_ops': round(thr, 2),
        'avg_us':         round(avg_us, 2),
        'p50_us':         round(p50_us, 2),
        'p90_us':         round(p90_us, 2),
        'p95_us':         round(p95_us, 2),
        'p99_us':         round(p99_us, 2),
        'run_id':         datetime.now().strftime('%Y%m%dT%H%M'),
        'notes':          notes,
    }


def append_row(csv_path, row):
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not csv_path.exists()
    with open(csv_path, 'a', newline='') as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if new_file:
            w.writeheader()
        w.writerow(row)


def write_rows(csv_path, rows):
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def sort_key(row):
    backend_order = {'rdmastorage': 0, 's3express': 1}
    try:
        servers = int(row['servers'])
    except (ValueError, TypeError):
        servers = 0
    return (backend_order.get(row['backend'], 99), servers, row['workload'])


def parse_override(args):
    overrides = {
        'backend':      args.backend,
        'workload':     args.workload.upper() if args.workload else None,
        'threads':      args.threads,
        'value_bytes':  args.value_bytes,
        'record_count': args.record_count,
        'notes':        args.notes,
    }
    for name in ('servers', 'k', 'm'):
        raw = getattr(args, name)
        if raw is None:
            overrides[name] = None
        elif str(raw).isdigit():
            overrides[name] = int(raw)
        else:
            overrides[name] = raw
    return overrides


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument('logfile', nargs='?',
                    help='single YCSB run-phase log to parse and append to CSV')
    ap.add_argument('--scan', metavar='DIR',
                    help='scan DIR for *_run.log files and rebuild the CSV from scratch')
    ap.add_argument('--csv', default='benchmarks/results/runs.csv')
    ap.add_argument('--backend', default=None)
    ap.add_argument('--workload', default=None)
    ap.add_argument('--servers', default=None)
    ap.add_argument('--k', default=None)
    ap.add_argument('--m', default=None)
    ap.add_argument('--threads', type=int, default=None)
    ap.add_argument('--value-bytes', type=int, default=None)
    ap.add_argument('--record-count', type=int, default=None)
    ap.add_argument('--notes', default=None)
    args = ap.parse_args()

    overrides = parse_override(args)
    csv_path = Path(args.csv)

    if args.scan:
        scan_dir = Path(args.scan)
        if not scan_dir.is_dir():
            sys.exit(f'--scan path is not a directory: {scan_dir}')
        rows = []
        for log in sorted(scan_dir.glob('*_run.log')):
            try:
                rows.append(build_row(log, overrides))
                print(f'  parsed {log.name}', file=sys.stderr)
            except Exception as e:
                print(f'  SKIP   {log.name}: {e}', file=sys.stderr)
        rows.sort(key=sort_key)
        write_rows(csv_path, rows)
        print(f'wrote {len(rows)} rows to {csv_path}', file=sys.stderr)
        return

    if not args.logfile:
        ap.error('logfile required unless --scan is given')

    try:
        row = build_row(args.logfile, overrides)
    except RuntimeError as e:
        print(f'ERROR: {e}', file=sys.stderr)
        sys.exit(1)

    append_row(csv_path, row)
    print(
        f'{row["backend"]} {row["workload"]} N={row["servers"]} '
        f'thr={row["throughput_ops"]} ops/s  avg={row["avg_us"]}us  '
        f'p95={row["p95_us"]}us  p99={row["p99_us"]}us  ops={row["op_count"]}  '
        f'-> {csv_path}',
        file=sys.stderr,
    )


if __name__ == '__main__':
    main()
