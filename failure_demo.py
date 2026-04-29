#!/usr/bin/env python3
import sys, os, time
sys.path.insert(0, os.path.expanduser('~/efa-kv-store/build'))
import rdmastorage

print("Connecting to coordinator...")
client = rdmastorage.Client('node0')
print(f"Connected: k={client.k}  m={client.m}  (tolerates {client.m} node failures)\n")

# Store a key before any failure
client.put('demo-key', b'hello from before the failure')
print("Stored 'demo-key'.\n")

print("=" * 50)
print("NOW KILL A SERVER NODE (e.g. pkill server on node2)")
print("=" * 50)

for i in range(30, 0, -1):
    print(f"  {i:2d}s remaining...", end='\r')
    time.sleep(1)
print("\nCountdown done.\n")

# Wait until the coordinator's DEAD notification has arrived.
# Failure detection takes ~6s (3 missed pings × 2s interval).
print("Waiting for DEAD notification from coordinator", end='', flush=True)
for _ in range(20):
    if client.dead_servers():
        break
    print(".", end='', flush=True)
    time.sleep(1)

dead = client.dead_servers()
if not dead:
    print("\nNo server marked dead yet — did you kill one? Exiting.")
    sys.exit(1)
print(f"\nServer(s) at position(s) {list(dead)} declared dead.\n")

# Test GET — should work via degraded read (erasure coding reconstructs from remaining shards)
print("Testing GET (degraded read)...")
try:
    val = client.get('demo-key')
    print(f"  GET succeeded: {val}  ✓")
except Exception as e:
    print(f"  GET failed: {e}  ✗")

# Test PUT — should fail immediately (can't write all shards)
print("\nTesting PUT on existing client...")
try:
    ok = client.put('demo-key-2', b'written after failure')
    if not ok:
        print("  PUT refused (server is dead)  ✓")
    else:
        print("  PUT succeeded unexpectedly")
except RuntimeError as e:
    print(f"  PUT raised: {e}  ✓")

# Close old client first — servers only accept one connection at a time
print("\nClosing old client...")
del client

# New client picks up reconfigured k/m
print("Creating new client with updated cluster config...")
client2 = rdmastorage.Client('node0')
print(f"  New client: k={client2.k}  m={client2.m}")
try:
    client2.put('demo-key-2', b'written after reconfiguration')
    val = client2.get('demo-key-2')
    print(f"  PUT + GET on new client succeeded: {val}  ✓")
except Exception as e:
    print(f"  Error: {e}")
