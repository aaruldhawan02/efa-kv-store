#!/usr/bin/env python3
"""plot_object_size_latency.py — render latency vs object size charts.

Produces PNG files in results/:
  1. latency_vs_size_avg.png  — average latency (µs) vs object size
  2. latency_vs_size_p50.png  — p50 latency (µs) vs object size
  3. latency_vs_size_p99.png  — p99 latency (µs) vs object size
  4. latency_vs_size_p999.png — p999 latency (µs) vs object size

Usage:
    python3 benchmarks/plot_object_size_latency.py
    python3 benchmarks/plot_object_size_latency.py --csv path/to/latency_by_size.csv --out path/to/dir
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


def load_rows(csv_path):
    """Load latency benchmark data from CSV."""
    rows = []
    with open(csv_path, newline='') as f:
        for r in csv.DictReader(f):
            for fld in ('object_size', 'avg_us', 'p50_us', 'p99_us', 'p999_us', 'bw_mbs'):
                try:
                    r[fld] = float(r[fld])
                except (TypeError, ValueError, KeyError):
                    r[fld] = 0.0
            rows.append(r)
    return rows


def latency_by_op_and_size(rows):
    """Group latency data by operation (PUT/GET) and object size.
    
    Returns: {op -> [(size_bytes, row), ...]}
    """
    by_op = defaultdict(list)
    for r in rows:
        op = r.get('op', '').upper()
        size = r.get('object_size', 0)
        if op and size:
            by_op[op].append((size, r))
    
    # Sort by size
    for op in by_op:
        by_op[op].sort(key=lambda t: t[0])
    
    return by_op


def human_size(b):
    """Convert bytes to human-readable string."""
    if b >= 1024 * 1024:
        return f"{b // (1024*1024)}MB"
    if b >= 1024:
        return f"{b // 1024}KB"
    return f"{b}B"


def plot_latency_vs_size(by_op, percentile_field, percentile_label, out_path):
    """Plot a single latency metric across all object sizes."""
    fig, ax = plt.subplots(figsize=(10, 6))
    
    plotted = False
    colors = {'PUT': 'tab:blue', 'GET': 'tab:orange'}
    
    for op in sorted(by_op.keys()):
        pts = by_op[op]
        if not pts:
            continue
        
        plotted = True
        xs = [p[0] for p in pts]  # object sizes
        ys = [p[1].get(percentile_field, 0.0) for p in pts]
        
        # Plot with markers
        ax.plot(xs, ys, marker='o', label=op, color=colors.get(op, 'tab:blue'), linewidth=2)
        
        # Add value labels on points
        for x, y in zip(xs, ys):
            if y > 0:
                ax.annotate(f'{y:.0f}', xy=(x, y), xytext=(0, 5),
                           textcoords='offset points', ha='center', fontsize=8)
    
    if not plotted:
        plt.close(fig)
        return False
    
    # Format x-axis with human-readable sizes
    size_labels = [human_size(s) for s in ax.get_xticks()]
    ax.set_xticklabels(size_labels)
    
    ax.set_xlabel('Object Size', fontsize=11)
    ax.set_ylabel(f'{percentile_label} Latency (µs)', fontsize=11)
    ax.set_title(f'RDMAStorage {percentile_label} Latency vs Object Size', fontsize=12)
    ax.grid(True, alpha=0.3, which='both')
    ax.legend(fontsize=10)
    
    # Use log scale for x-axis if helpful
    try:
        ax.set_xscale('log')
    except:
        pass
    
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return True


def main():
    here = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument('--csv', default=str(here / 'results' / 'latency_by_size.csv'))
    ap.add_argument('--out', default=str(here / 'results'))
    args = ap.parse_args()

    csv_path = Path(args.csv)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not csv_path.exists():
        sys.exit(f'no CSV at {csv_path} — run bench_latency.py first')

    rows = load_rows(csv_path)
    by_op = latency_by_op_and_size(rows)

    targets = [
        ('latency_vs_size_avg.png',   'avg_us',   'Average'),
        ('latency_vs_size_p50.png',   'p50_us',   'p50'),
        ('latency_vs_size_p99.png',   'p99_us',   'p99'),
        ('latency_vs_size_p999.png',  'p999_us',  'p999'),
    ]

    for fname, field, label in targets:
        out = out_dir / fname
        if plot_latency_vs_size(by_op, field, label, out):
            print(f'wrote {out}')
        else:
            print(f'skipped {fname} — not enough data', file=sys.stderr)


if __name__ == '__main__':
    main()
