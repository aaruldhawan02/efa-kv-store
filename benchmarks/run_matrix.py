#!/usr/bin/env python3
"""run_matrix.py — drive the YCSB matrix from a controller outside the cluster.

For every (N, workload) cell in the matrix, this script:

  1. ssh's into COORD_NODE + SERVER_NODES and kills leftover processes
  2. starts ./build/coordinator on COORD_NODE
  3. starts ./build/server --coord COORD_NODE on the first N server nodes
  4. polls the coordinator log over ssh until it reports alive=N
  5. runs `ycsb load` then `ycsb run` on COORD_NODE, tee'ing stdout to
     {REPO_REMOTE}/benchmarks/results/raw/rdma_<W>_N<N>_<phase>.log
  6. runs parse_ycsb_output.py over ssh — one row appended to
     {REPO_REMOTE}/benchmarks/results/runs.csv

At the end (unless --no-pull) the CSV and raw/ are scp'd back to this
machine into benchmarks/results/ next to the script.

Prereqs on the controller: ssh access (key-based or ssh-agent) to every
host in COORD_NODE + SERVER_NODES below. Edit CONFIG before first use.

Usage:
    python3 benchmarks/run_matrix.py                       # full 6x5 sweep
    python3 benchmarks/run_matrix.py --ns "3 6" --workloads "a c"
    python3 benchmarks/run_matrix.py --threads 16 --ops 50000
    python3 benchmarks/run_matrix.py --pull                # just scp results
"""

import argparse
import re
import subprocess
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# CONFIG — edit these for your cluster
# ---------------------------------------------------------------------------

# ssh-resolvable hostnames (or aliases from ~/.ssh/config).
COORD_NODE   = 'hp046.utah.cloudlab.us'   # node0: coordinator + YCSB client
SERVER_NODES = [
    'hp076.utah.cloudlab.us',             # node1
    'hp064.utah.cloudlab.us',             # node2
    'hp070.utah.cloudlab.us',             # node3
    'hp074.utah.cloudlab.us',             # node4
    'hp066.utah.cloudlab.us',             # node5
    'hp065.utah.cloudlab.us',             # node6
]

# Paths on the cluster nodes — same layout on every host.
# Use $HOME (not ~) so the value also expands inside double-quoted strings.
REPO_REMOTE  = '$HOME/RDMA-Distributed-KV-Store'
YCSB_REMOTE  = f'{REPO_REMOTE}/ycsb-0.17.0'
BUILD_REMOTE = f'{REPO_REMOTE}/build'

# Default sweep; --ns / --workloads override.
# N=1 is excluded — the coordinator requires at least 2 live servers
# (coordinator.cpp:200 hardcodes `WAIT %d/2`).
NS_DEFAULT        = [2, 3, 4, 5, 6]
WORKLOADS_DEFAULT = ['a', 'b', 'c', 'd', 'f']

# Default per-cell YCSB knobs; CLI overrides
RECORDS     = 50000
OPS         = 10000
VALUE_BYTES = 1024
THREADS     = 1
# -Djava.library.path lets the JVM find librdmastorage_jni.so;
# LD_LIBRARY_PATH (exported separately below) lets that .so dlopen libfabric/libisal.
JVM_ARGS    = (
    '-XX:TieredStopAtLevel=1 -XX:ErrorFile=/tmp/hs_err_%p.log '
    f'-Djava.library.path={BUILD_REMOTE}'
)

# ---------------------------------------------------------------------------

SSH_OPTS = [
    '-o', 'BatchMode=yes',
    '-o', 'StrictHostKeyChecking=accept-new',
    '-o', 'ConnectTimeout=5',
]

LOCAL_RESULTS = Path(__file__).resolve().parent / 'results'


def ts():
    return time.strftime('%H:%M:%S')


def log(msg):
    print(f'[{ts()}] {msg}', flush=True)


def ssh_run(host, cmd, capture=False, stream=False):
    """Run cmd on host over ssh, waiting for completion.

    stream=True forwards stdout/stderr to the controller's terminal (useful
    for long YCSB runs). capture=True returns (rc, output) where output is
    stdout+stderr merged (so remote error messages aren't silently dropped).
    """
    full = ['ssh'] + SSH_OPTS + [host, cmd]
    if capture:
        r = subprocess.run(full, stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT, text=True)
        return r.returncode, r.stdout
    if stream:
        return subprocess.call(full)
    return subprocess.call(full, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)


def ssh_detach(host, cmd):
    """Launch a backgrounded remote process and return immediately.

    Wraps cmd in a subshell that closes stdin and disowns, so ssh returns
    as soon as the remote command is in flight.
    """
    wrapped = f'( {cmd} ) </dev/null >/dev/null 2>&1 &'
    full = ['ssh', '-n'] + SSH_OPTS + [host, wrapped]
    return subprocess.Popen(full)


def tear_down(servers):
    log(f'tear_down: coord + {len(servers)} server(s)')
    procs = [ssh_detach(COORD_NODE, 'pkill -9 -f build/coordinator 2>/dev/null; true')]
    for s in servers:
        procs.append(ssh_detach(s, 'pkill -9 -f build/server 2>/dev/null; true'))
    for p in procs:
        p.wait()
    time.sleep(2)


def bring_up(n):
    coord_log = f'{REPO_REMOTE}/benchmarks/results/raw/coord_N{n}.log'
    log(f'bring_up: coordinator on {COORD_NODE} -> {coord_log}')
    ssh_detach(
        COORD_NODE,
        f'cd {REPO_REMOTE} && mkdir -p benchmarks/results/raw && '
        f'rm -f {coord_log} && '
        f'nohup ./build/coordinator > {coord_log} 2>&1'
    ).wait()
    time.sleep(1)

    log(f'bring_up: {n} server(s)')
    procs = []
    for s in SERVER_NODES[:n]:
        procs.append(ssh_detach(
            s,
            f'cd {REPO_REMOTE} && '
            f'nohup ./build/server --coord {COORD_NODE} > /tmp/server.log 2>&1'
        ))
    for p in procs:
        p.wait()

    log(f'bring_up: waiting for alive={n} (max 60s)')
    for _ in range(60):
        rc, out = ssh_run(
            COORD_NODE,
            f"grep 'alive={n} ' {coord_log} 2>/dev/null | tail -1",
            capture=True,
        )
        if out.strip():
            log(f'bring_up: {out.strip()}')
            return True
        time.sleep(1)

    rc, tail = ssh_run(COORD_NODE, f'tail -n 20 {coord_log}', capture=True)
    log(f'ERROR: cluster did not reach alive={n}; coord tail:\n{tail}')
    return False


def remote_log_complete(log_relpath):
    """True if {REPO_REMOTE}/log_relpath exists on COORD_NODE and contains [OVERALL]."""
    cmd = (
        f'grep -q "^\\[OVERALL\\]" {REPO_REMOTE}/{log_relpath} 2>/dev/null '
        f'&& echo OK || echo NO'
    )
    rc, out = ssh_run(COORD_NODE, cmd, capture=True)
    return out.strip() == 'OK'


def run_cell(n, workload):
    """Run load + run for one cell. Return True only if both produced [OVERALL].

    The exit code of `ycsb | tee` is the tee's, not YCSB's, so a YCSB
    SIGSEGV can leave the pipe rc=0. We verify success by grepping the log
    for [OVERALL] afterwards.
    """
    wu = workload.upper()
    wl = workload.lower()
    raw = 'benchmarks/results/raw'
    load_log = f'{raw}/rdma_{wu}_N{n}_load.log'
    run_log  = f'{raw}/rdma_{wu}_N{n}_run.log'

    def ycsb_cmd(phase, op_arg, log_path):
        # histogram.percentiles makes the [READ]/[UPDATE]/etc. summary blocks
        # emit 50/90/95/99 percentile lines (default is just 95,99).
        return (
            f'cd {REPO_REMOTE} && mkdir -p {raw} && '
            f'export LD_LIBRARY_PATH={BUILD_REMOTE}:$LD_LIBRARY_PATH && '
            f'{YCSB_REMOTE}/bin/ycsb {phase} rdmastorage '
            f'-jvm-args="{JVM_ARGS}" '
            f'-P {YCSB_REMOTE}/workloads/workload{wl} '
            f'-p rdmastorage.coord={COORD_NODE} '
            f'-p recordcount={RECORDS} {op_arg} '
            f'-p fieldcount=1 -p fieldlength={VALUE_BYTES} '
            f'-p histogram.percentiles=50,90,95,99 '
            f'-threads {THREADS} -s 2>&1 | tee {log_path} >/dev/null'
        )

    # Run load. JNI crashes during cleanup happen AFTER inserts succeed;
    # the data still made it. Don't gate on load.log having [OVERALL].
    log(f'cell N={n} W={wu}: load')
    ssh_run(COORD_NODE, ycsb_cmd('load', '', load_log), stream=True)

    log(f'cell N={n} W={wu}: run')
    ssh_run(COORD_NODE,
            ycsb_cmd('run', f'-p operationcount={OPS}', run_log),
            stream=True)
    if not remote_log_complete(run_log):
        log(f'WARN: run N={n} W={wu} did not finish (no [OVERALL] in log)')
        return False

    # Sanity: a load that actually lost records shows up as Return=NOT_FOUND
    # in the run summary. If any NF lines exist, the load really failed.
    rc, out = ssh_run(
        COORD_NODE,
        f'grep -c "Return=NOT_FOUND" {REPO_REMOTE}/{run_log} 2>/dev/null; true',
        capture=True,
    )
    nf_lines = int(out.strip().splitlines()[-1] or 0) if out.strip() else 0
    if nf_lines > 0:
        log(f'WARN: run N={n} W={wu} has NOT_FOUND reads — load did not populate, retrying')
        return False

    log(f'cell N={n} W={wu}: parse')
    parse_cmd = (
        f'cd {REPO_REMOTE} && '
        f'python3 benchmarks/parse_ycsb_output.py {run_log} '
        f'--threads {THREADS} --value-bytes {VALUE_BYTES} '
        f'--record-count {RECORDS}'
    )
    rc, out = ssh_run(COORD_NODE, parse_cmd, capture=True)
    if out:
        sys.stderr.write(out if out.endswith('\n') else out + '\n')
    if rc != 0:
        log(f'WARN: parse N={n} W={wu} failed (rc={rc}) — see output above')
    return rc == 0


def execute_cell_with_retry(n, workload, retries):
    """Bring up a fresh cluster for this cell, run it, retry up to `retries` times."""
    for attempt in range(1, retries + 1):
        log(f'cell N={n} W={workload.upper()}: attempt {attempt}/{retries}')
        tear_down(SERVER_NODES[:n])
        if not bring_up(n):
            log(f'cell N={n} W={workload.upper()}: bring_up failed, retrying')
            continue
        if run_cell(n, workload):
            return True
        log(f'cell N={n} W={workload.upper()}: attempt {attempt} did not produce a complete log')
    return False


def discover_done_cells():
    """Return {(n, W_upper)} for cells whose run.log already has [OVERALL]."""
    cmd = (
        f'cd {REPO_REMOTE} 2>/dev/null && '
        f'for f in benchmarks/results/raw/rdma_*_N*_run.log; do '
        f'  [ -f "$f" ] && grep -q "^\\[OVERALL\\]" "$f" && echo "$f"; '
        f'done 2>/dev/null'
    )
    rc, out = ssh_run(COORD_NODE, cmd, capture=True)
    pat = re.compile(r'rdma_([A-Z])_N(\d+)_run\.log$')
    done = set()
    for line in out.splitlines():
        m = pat.search(line.strip())
        if m:
            done.add((int(m.group(2)), m.group(1)))
    return done


def sync_parse_script():
    """scp the local parse_ycsb_output.py to the coord node before each sweep,
    so a stale remote copy can't silently break --threads / filename-inference
    behaviour."""
    local = Path(__file__).resolve().parent / 'parse_ycsb_output.py'
    if not local.exists():
        log(f'WARN: {local} missing — cannot sync to {COORD_NODE}')
        return
    scp_repo = REPO_REMOTE.replace('$HOME', '~').replace('${HOME}', '~')
    dst = f'{COORD_NODE}:{scp_repo}/benchmarks/parse_ycsb_output.py'
    log(f'syncing parse_ycsb_output.py -> {COORD_NODE}')
    rc = subprocess.call(['scp', '-q'] + SSH_OPTS + [str(local), dst])
    if rc != 0:
        log(f'WARN: scp of parse_ycsb_output.py returned rc={rc}')


def pull_results():
    LOCAL_RESULTS.mkdir(parents=True, exist_ok=True)
    log(f'pulling results back to {LOCAL_RESULTS}/')
    # scp doesn't run a remote shell, so $HOME isn't expanded in the path.
    # Tilde works for both old-protocol scp and modern SFTP-mode scp.
    scp_repo = REPO_REMOTE.replace('$HOME', '~').replace('${HOME}', '~')
    src_csv = f'{COORD_NODE}:{scp_repo}/benchmarks/results/runs.csv'
    src_raw = f'{COORD_NODE}:{scp_repo}/benchmarks/results/raw'
    subprocess.call(['scp'] + SSH_OPTS + [src_csv, str(LOCAL_RESULTS)])
    subprocess.call(['scp', '-r'] + SSH_OPTS + [src_raw, str(LOCAL_RESULTS)])


def main():
    global RECORDS, OPS, VALUE_BYTES, THREADS
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument('--ns', type=str, default=None,
                    help='space-separated N values, e.g. "1 3 6" (default: 1..6)')
    ap.add_argument('--workloads', type=str, default=None,
                    help='space-separated workload letters, e.g. "a c" (default: a b c d f)')
    ap.add_argument('--records',     type=int, default=RECORDS)
    ap.add_argument('--ops',         type=int, default=OPS)
    ap.add_argument('--value-bytes', type=int, default=VALUE_BYTES)
    ap.add_argument('--threads',     type=int, default=THREADS)
    ap.add_argument('--pull',    action='store_true', help='just scp results back and exit')
    ap.add_argument('--no-pull', action='store_true', help='skip the post-sweep scp')
    ap.add_argument('--force',   action='store_true',
                    help='re-run every cell even if its run.log already exists on the cluster')
    ap.add_argument('--retries', type=int, default=2,
                    help='attempts per cell before giving up (default: 2)')
    args = ap.parse_args()

    if args.pull:
        pull_results()
        return

    sync_parse_script()

    RECORDS = args.records
    OPS = args.ops
    VALUE_BYTES = args.value_bytes
    THREADS = args.threads

    ns = [int(x) for x in args.ns.split()] if args.ns else NS_DEFAULT
    workloads = args.workloads.split() if args.workloads else WORKLOADS_DEFAULT

    log(f'matrix: NS={ns}  WORKLOADS={workloads}')
    log(f'matrix: RECORDS={RECORDS} OPS={OPS} VALUE_BYTES={VALUE_BYTES} THREADS={THREADS}')
    log(f'matrix: COORD={COORD_NODE}  SERVERS={SERVER_NODES[:max(ns)] if ns else []}')

    done = set() if args.force else discover_done_cells()
    if done:
        log(f'found {len(done)} completed cell(s) on cluster — will skip those: '
            f'{sorted(done)}')

    # Decide what's left to run, per N.
    todo_by_n = {}
    for n in ns:
        if n > len(SERVER_NODES):
            log(f'skip N={n}: only {len(SERVER_NODES)} entries in SERVER_NODES')
            continue
        missing = [w for w in workloads if (n, w.upper()) not in done]
        if missing:
            todo_by_n[n] = missing

    if not todo_by_n:
        log('matrix: nothing to run — every requested cell is already on the cluster')
        if not args.no_pull:
            pull_results()
        return

    failed = []
    try:
        for n, missing in todo_by_n.items():
            log(f'N={n}: running workloads {missing}')
            for w in missing:
                if not execute_cell_with_retry(n, w, args.retries):
                    failed.append((n, w.upper()))
                    log(f'GIVING UP on N={n} W={w.upper()} after {args.retries} attempts')
    finally:
        tear_down(SERVER_NODES)

    if failed:
        log(f'matrix: {len(failed)} cell(s) never produced a complete log: {failed}')
    else:
        log('matrix: all requested cells completed')

    if not args.no_pull:
        pull_results()


if __name__ == '__main__':
    main()
