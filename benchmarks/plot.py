#!/usr/bin/env python3
"""plot.py — render benchmark charts from results/runs.csv.

Produces up to three PNGs in results/:
  1. throughput_vs_n.png — ops/sec vs server count (rdmastorage rows), one line
                           per workload. Each marker annotated with (k, m).
  2. p99_vs_n.png        — p99 latency (µs) vs server count, same shape.
  3. rdma_vs_s3.png      — grouped bar: rdmastorage @ max-N vs s3express, per
                           workload. Each bar labelled with its p99.

Usage:
    python3 benchmarks/plot.py
    python3 benchmarks/plot.py --csv path/to/runs.csv --out path/to/dir
"""

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
except ImportError:
    sys.exit('matplotlib not installed: pip install matplotlib')

WORKLOADS = ['A', 'B', 'C', 'D', 'F']
WORKLOAD_DESC = {
    'A': 'A: 50/50 read/update',
    'B': 'B: 95/5 read/update',
    'C': 'C: 100% read',
    'D': 'D: 95/5 read/insert (latest)',
    'F': 'F: 50/50 read/rmw',
}
WORKLOAD_COLORS = {
    'A': 'tab:blue',
    'B': 'tab:orange',
    'C': 'tab:green',
    'D': 'tab:red',
    'F': 'tab:purple',
}


def int_or(v, default=None):
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def load_rows(csv_path):
    rows = []
    with open(csv_path, newline='') as f:
        for r in csv.DictReader(f):
            for fld in ('throughput_ops', 'avg_us',
                        'p50_us', 'p90_us', 'p95_us', 'p99_us'):
                try:
                    r[fld] = float(r[fld])
                except (TypeError, ValueError, KeyError):
                    r[fld] = 0.0
            rows.append(r)
    return rows


def rdma_by_workload(rows):
    by_w = defaultdict(list)  # workload -> [(N, row), ...]
    for r in rows:
        if r['backend'] != 'rdmastorage':
            continue
        n = int_or(r['servers'])
        if n is None:
            continue
        by_w[r['workload']].append((n, r))
    for w in by_w:
        by_w[w].sort(key=lambda t: t[0])
    return by_w


def plot_throughput_vs_n(by_w, out_path):
    fig, ax = plt.subplots(figsize=(8, 5))
    plotted = False
    for w in WORKLOADS:
        pts = by_w.get(w, [])
        if not pts:
            continue
        plotted = True
        xs = [n for n, _ in pts]
        ys = [r['throughput_ops'] for _, r in pts]
        ax.plot(xs, ys, marker='o', label=WORKLOAD_DESC.get(w, w),
                color=WORKLOAD_COLORS.get(w))
        for n, r in pts:
            k, m = r.get('k'), r.get('m')
            if k not in (None, '', 'N/A') and m not in (None, '', 'N/A'):
                ax.annotate(f'({k},{m})',
                            xy=(n, r['throughput_ops']),
                            xytext=(4, 4), textcoords='offset points',
                            fontsize=8)
    if not plotted:
        plt.close(fig)
        return False
    ax.set_xlabel('Servers (N)')
    ax.set_ylabel('Throughput (ops/sec)')
    ax.set_title('RDMAStorage throughput vs cluster size  — markers annotated with (k, m)')
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return True


def plot_percentile_vs_n(by_w, percentile_field, label, out_path):
    """One PNG per percentile, one line per workload, missing values dropped."""
    fig, ax = plt.subplots(figsize=(8, 5))
    plotted = False
    for w in WORKLOADS:
        pts = by_w.get(w, [])
        # Drop points where this percentile is missing (0.0 = sentinel from parse).
        pts = [(n, r) for n, r in pts if r.get(percentile_field, 0.0) > 0.0]
        if not pts:
            continue
        plotted = True
        xs = [n for n, _ in pts]
        ys = [r[percentile_field] for _, r in pts]
        ax.plot(xs, ys, marker='o', label=WORKLOAD_DESC.get(w, w),
                color=WORKLOAD_COLORS.get(w))
    if not plotted:
        plt.close(fig)
        return False
    ax.set_xlabel('Servers (N)')
    ax.set_ylabel(f'{label} latency (µs)')
    ax.set_title(f'RDMAStorage {label} latency vs cluster size')
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return True


def plot_rdma_vs_s3(rows, out_path):
    rdma = [r for r in rows
            if r['backend'] == 'rdmastorage' and int_or(r['servers']) is not None]
    s3   = [r for r in rows if r['backend'] == 's3express']
    if not rdma or not s3:
        return False

    max_n = max(int_or(r['servers']) for r in rdma)
    rdma_best, s3_best = {}, {}
    for r in rdma:
        if int_or(r['servers']) == max_n:
            rdma_best.setdefault(r['workload'], r)
    for r in s3:
        s3_best.setdefault(r['workload'], r)

    workloads = [w for w in WORKLOADS if w in rdma_best and w in s3_best]
    if not workloads:
        return False

    fig, ax = plt.subplots(figsize=(8, 5))
    width = 0.38
    xs = list(range(len(workloads)))
    rdma_y   = [rdma_best[w]['throughput_ops'] for w in workloads]
    s3_y     = [s3_best[w]['throughput_ops']   for w in workloads]
    rdma_p99 = [rdma_best[w]['p99_us']         for w in workloads]
    s3_p99   = [s3_best[w]['p99_us']           for w in workloads]

    bars1 = ax.bar([x - width/2 for x in xs], rdma_y, width,
                   label=f'RDMAStorage (N={max_n})', color='tab:blue')
    bars2 = ax.bar([x + width/2 for x in xs], s3_y, width,
                   label='S3 Express',                color='tab:gray')

    for bar, p99 in list(zip(bars1, rdma_p99)) + list(zip(bars2, s3_p99)):
        ax.annotate(f'p99={p99:.0f}µs',
                    xy=(bar.get_x() + bar.get_width()/2, bar.get_height()),
                    xytext=(0, 3), textcoords='offset points',
                    ha='center', fontsize=8)

    ax.set_xticks(xs)
    ax.set_xticklabels(workloads)
    ax.set_xlabel('Workload')
    ax.set_ylabel('Throughput (ops/sec)')
    ax.set_title(f'RDMAStorage (N={max_n}) vs S3 Express — labels show p99 (µs)')
    ax.grid(True, axis='y', alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return True


def main():
    here = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument('--csv', default=str(here / 'results' / 'runs.csv'))
    ap.add_argument('--out', default=str(here / 'results'))
    args = ap.parse_args()

    csv_path = Path(args.csv)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not csv_path.exists():
        sys.exit(f'no CSV at {csv_path} — run the benchmarks first')

    rows = load_rows(csv_path)
    by_w = rdma_by_workload(rows)

    targets = [
        ('throughput_vs_n.png', plot_throughput_vs_n, (by_w,)),
        ('p50_vs_n.png',        plot_percentile_vs_n, (by_w, 'p50_us', 'p50')),
        ('p90_vs_n.png',        plot_percentile_vs_n, (by_w, 'p90_us', 'p90')),
        ('p95_vs_n.png',        plot_percentile_vs_n, (by_w, 'p95_us', 'p95')),
        ('p99_vs_n.png',        plot_percentile_vs_n, (by_w, 'p99_us', 'p99')),
        ('rdma_vs_s3.png',      plot_rdma_vs_s3,      (rows,)),
    ]
    for name, fn, args_ in targets:
        out = out_dir / name
        if fn(*args_, out):
            print(f'wrote {out}')
        else:
            print(f'skipped {name} — not enough data', file=sys.stderr)


if __name__ == '__main__':
    main()
