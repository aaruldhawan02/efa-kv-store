#!/usr/bin/env python3
"""plot_phases.py — render RDMA phase breakdown charts.

Produces PNG files in results/:
  1. phases_put.png  — PUT phase breakdown (stacked bar or breakdown chart)
  2. phases_get.png  — GET phase breakdown (stacked bar or breakdown chart)
  3. phases_rdma_focus.png — focus on RDMA phase latency for PUT/GET

Usage:
    python3 benchmarks/plot_phases.py
    python3 benchmarks/plot_phases.py --csv path/to/phases.csv --out path/to/dir
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
    import numpy as np
except ImportError:
    sys.exit('matplotlib not installed: pip install matplotlib')


def load_rows(csv_path):
    """Load phase breakdown data from CSV."""
    rows = []
    with open(csv_path, newline='') as f:
        for r in csv.DictReader(f):
            for fld in ('object_bytes', 'avg_us', 'p50_us', 'p99_us'):
                try:
                    r[fld] = float(r[fld])
                except (TypeError, ValueError, KeyError):
                    r[fld] = 0.0
            rows.append(r)
    return rows


def phases_by_op(rows):
    """Group phase data by operation.
    
    Returns: {op -> {phase -> row}}
    """
    by_op = defaultdict(dict)
    for r in rows:
        op = r.get('op', '').upper()
        phase = r.get('phase', '')
        if op and phase:
            by_op[op][phase] = r
    return by_op


def plot_phase_breakdown(by_op, op, out_path):
    """Plot phase breakdown as grouped bar chart for an operation."""
    if op not in by_op:
        return False
    
    phases_dict = by_op[op]
    if not phases_dict:
        return False
    
    # Get phase names, excluding 'total'
    phase_names = [p for p in phases_dict.keys() if p.lower() != 'total']
    if not phase_names:
        return False
    
    # Get total for reference
    total_row = phases_dict.get('total', {})
    total_avg = total_row.get('avg_us', 1.0)
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # Extract data
    avg_values = [phases_dict[p].get('avg_us', 0) for p in phase_names]
    p99_values = [phases_dict[p].get('p99_us', 0) for p in phase_names]
    
    # Normalize phase names for display
    display_names = [name.replace('_', ' ').title() for name in phase_names]
    
    # Color scheme
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
    phase_colors = {name: colors[i % len(colors)] for i, name in enumerate(phase_names)}
    bar_colors = [phase_colors[p] for p in phase_names]
    
    # Chart 1: Average latency by phase
    x_pos = np.arange(len(phase_names))
    bars1 = ax1.bar(x_pos, avg_values, color=bar_colors, alpha=0.8, edgecolor='black', linewidth=1.5)
    
    # Add value labels on bars
    for bar, val in zip(bars1, avg_values):
        height = bar.get_height()
        pct = (val / total_avg * 100) if total_avg > 0 else 0
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{val:.1f}µs\n({pct:.1f}%)',
                ha='center', va='bottom', fontsize=9)
    
    ax1.set_ylabel('Latency (µs)', fontsize=11)
    ax1.set_title(f'{op} Operation: Average Phase Latencies', fontsize=12, fontweight='bold')
    ax1.set_xticks(x_pos)
    ax1.set_xticklabels(display_names, rotation=45, ha='right')
    ax1.grid(True, axis='y', alpha=0.3)
    
    # Chart 2: p99 latency by phase
    bars2 = ax2.bar(x_pos, p99_values, color=bar_colors, alpha=0.8, edgecolor='black', linewidth=1.5)
    
    # Add value labels on bars
    for bar, val in zip(bars2, p99_values):
        height = bar.get_height()
        pct = (val / total_row.get('p99_us', 1.0) * 100) if total_row.get('p99_us', 0) > 0 else 0
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{val:.1f}µs\n({pct:.1f}%)',
                ha='center', va='bottom', fontsize=9)
    
    ax2.set_ylabel('Latency (µs)', fontsize=11)
    ax2.set_title(f'{op} Operation: p99 Phase Latencies', fontsize=12, fontweight='bold')
    ax2.set_xticks(x_pos)
    ax2.set_xticklabels(display_names, rotation=45, ha='right')
    ax2.grid(True, axis='y', alpha=0.3)
    
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return True


def plot_rdma_focus(by_op, out_path):
    """Plot RDMA phase latencies across PUT and GET operations."""
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Find RDMA phase data for both operations
    put_phases = by_op.get('PUT', {})
    get_phases = by_op.get('GET', {})
    
    # Look for RDMA phases (rdma_write for PUT, rdma_read for GET)
    put_rdma_key = None
    get_rdma_key = None
    
    for key in put_phases.keys():
        if 'rdma' in key.lower() and 'write' in key.lower():
            put_rdma_key = key
            break
    for key in put_phases.keys():
        if 'rdma' in key.lower():
            put_rdma_key = key
            break
    
    for key in get_phases.keys():
        if 'rdma' in key.lower() and 'read' in key.lower():
            get_rdma_key = key
            break
    for key in get_phases.keys():
        if 'rdma' in key.lower():
            get_rdma_key = key
            break
    
    if not put_rdma_key or not get_rdma_key:
        plt.close(fig)
        return False
    
    # Extract RDMA data
    put_avg = put_phases[put_rdma_key].get('avg_us', 0)
    put_p99 = put_phases[put_rdma_key].get('p99_us', 0)
    get_avg = get_phases[get_rdma_key].get('avg_us', 0)
    get_p99 = get_phases[get_rdma_key].get('p99_us', 0)
    
    # Create grouped bar chart
    operations = ['PUT', 'GET']
    x_pos = np.arange(len(operations))
    width = 0.35
    
    avg_vals = [put_avg, get_avg]
    p99_vals = [put_p99, get_p99]
    
    bars1 = ax.bar(x_pos - width/2, avg_vals, width, label='Average', 
                   color='tab:blue', alpha=0.8, edgecolor='black', linewidth=1.5)
    bars2 = ax.bar(x_pos + width/2, p99_vals, width, label='p99',
                   color='tab:orange', alpha=0.8, edgecolor='black', linewidth=1.5)
    
    # Add value labels
    for bar in bars1:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
               f'{height:.1f}µs', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    for bar in bars2:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
               f'{height:.1f}µs', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    ax.set_ylabel('Latency (µs)', fontsize=11)
    ax.set_title('RDMA Phase Latencies: PUT (write) vs GET (read)', fontsize=12, fontweight='bold')
    ax.set_xticks(x_pos)
    ax.set_xticklabels(operations)
    ax.legend(fontsize=10)
    ax.grid(True, axis='y', alpha=0.3)
    
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return True


def main():
    here = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument('--csv', default=str(here / 'results' / 'phases.csv'))
    ap.add_argument('--out', default=str(here / 'results'))
    args = ap.parse_args()

    csv_path = Path(args.csv)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not csv_path.exists():
        sys.exit(f'no CSV at {csv_path} — run bench_phases.py first')

    rows = load_rows(csv_path)
    by_op = phases_by_op(rows)

    if not by_op:
        sys.exit('no phase data found in CSV')

    targets = [
        ('phases_put.png',        'PUT'),
        ('phases_get.png',        'GET'),
        ('phases_rdma_focus.png', None),  # Special case
    ]

    for fname, op in targets:
        out = out_dir / fname
        if op is None:
            # Special case: RDMA focus chart
            success = plot_rdma_focus(by_op, out)
        else:
            success = plot_phase_breakdown(by_op, op, out)
        
        if success:
            print(f'wrote {out}')
        else:
            print(f'skipped {fname} — not enough data', file=sys.stderr)


if __name__ == '__main__':
    main()
