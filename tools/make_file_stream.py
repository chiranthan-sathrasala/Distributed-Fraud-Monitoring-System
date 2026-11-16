#!/usr/bin/env python3
"""Turn the dataset into a folder of small JSON-lines files (one file = one micro-batch).

Lets you test the whole Spark job WITHOUT Kafka or a cluster (see tools/dry_run.sh).
Files are numbered in delivery order, so disorder modes behave like the Kafka producer.
"""
import argparse, json, os, shutil, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from common import replay

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
p = argparse.ArgumentParser()
p.add_argument("--csv", default=os.path.join(ROOT, "dataset", "input", "transactions.csv"))
p.add_argument("--out", required=True, help="output folder (recreated)")
p.add_argument("--mode", default="normal", choices=list(replay.MODES))
p.add_argument("--seed", type=int, default=42)
p.add_argument("--events-per-file", type=int, default=10)
p.add_argument("--disorder-log", default=None, help="write the delayed transactions to this CSV")
a = p.parse_args()

items = replay.build_schedule(replay.load_events(a.csv), a.mode, a.seed)
shutil.rmtree(a.out, ignore_errors=True)
os.makedirs(a.out)
n = a.events_per_file
files = 0
# Spark's file source reads files ordered by MODIFICATION TIME (not by name). Give every file its
# own, strictly increasing timestamp so the delivery order is exactly the order we built.
n_files = (len(items) + n - 1) // n
base = time.time() - n_files - 5
for i in range(0, len(items), n):
    path = os.path.join(a.out, f"part-{i // n:05d}.json")
    with open(path, "w") as f:
        for it in items[i:i + n]:
            f.write(json.dumps(it["event"]) + "\n")
    os.utime(path, (base + files, base + files))
    files += 1
print(f"wrote {len(items)} events (incl. 1 flush marker) into {files} files in {a.out}")
delayed = replay.disorder_rows(items)
print(f"mode={a.mode}: {len(delayed)} delayed events")
if a.disorder_log:
    import csv
    with open(a.disorder_log, "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["transaction_id", "event_timestamp", "delay_seconds"])
        w.writerows(delayed)
    print(f"disorder log: {a.disorder_log}")
