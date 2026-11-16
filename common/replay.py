"""Shared replay logic used by the Kafka producer (VM1) and the file-stream tool.

Key idea: disorder is created ONLY by changing WHEN an event is delivered.
The event itself (including event_timestamp) is never modified, so the output
can still be compared with the ground truth.
"""
import csv
import random
from datetime import datetime, timedelta, timezone

FLUSH_ACCOUNT = "__FLUSH__"          # marker account for the end-of-stream event
FLUSH_OFFSET_SECONDS = 1800          # flush event is stamped this far after the last real event

# mode -> (fraction of events delayed, min delay s, max delay s)
MODES = {
    "normal": None,
    # Experiment 3: late by LESS than the 60 s watermark -> must still count
    "late_lt1m": (0.15, 10, 50),
    # Experiment 4: late by MORE than the 60 s watermark -> Spark may drop them
    "late_gt1m": (0.10, 300, 600),
}


def parse_ts(text):
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def load_events(path):
    """Read the CSV in file order (never sorted). Every value stays a string."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        return [dict(r) for r in csv.DictReader(f)]


def make_flush_event(events):
    """Synthetic last event that pushes the watermark forward so the final windows close.

    Spark only emits a window once the watermark passes its end, and the watermark only
    moves when newer data arrives. Without this marker the last ~3 minutes of the dataset
    would never be emitted. The Spark job ignores the marker's account in all outputs.
    """
    last = max(parse_ts(e["event_timestamp"]) for e in events)
    stamp = (last + timedelta(seconds=FLUSH_OFFSET_SECONDS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    flush = {k: "" for k in events[0].keys()}
    flush.update({"event_id": "FLUSH", "transaction_id": "__FLUSH__0001",
                  "source_account": FLUSH_ACCOUNT, "destination_account": FLUSH_ACCOUNT,
                  "event_timestamp": stamp, "amount": "0.00", "currency": "INR"})
    return flush


def build_schedule(events, mode="normal", seed=42, fraction=None, lo=None, hi=None, add_flush=True):
    """Return a list of dicts {event, pos, delay, seq} in DELIVERY order.

    pos = delivery position on the event-time axis (seconds). Normal events are delivered at
    their own event time; delayed events are delivered `delay` seconds later.
    """
    if mode not in MODES:
        raise ValueError(f"unknown mode {mode}; choose from {list(MODES)}")
    cfg = MODES[mode]
    if cfg is not None:
        fraction = cfg[0] if fraction is None else fraction
        lo = cfg[1] if lo is None else lo
        hi = cfg[2] if hi is None else hi
    rng = random.Random(seed)
    items = []
    for seq, e in enumerate(events):
        ts = parse_ts(e["event_timestamp"]).timestamp()
        delay = 0.0
        if cfg is not None and rng.random() < fraction:
            delay = round(rng.uniform(lo, hi), 1)
        items.append({"event": e, "pos": ts + delay, "delay": delay, "seq": seq})
    if cfg is not None:                       # normal mode keeps pure file order
        items.sort(key=lambda x: (x["pos"], x["seq"]))
    if add_flush:
        flush = make_flush_event(events)
        items.append({"event": flush, "pos": items[-1]["pos"] + 1.0, "delay": 0.0, "seq": len(items)})
    return items


def disorder_rows(items):
    """Rows for the disorder log: which transactions were delayed and by how much."""
    return [(i["event"]["transaction_id"], i["event"]["event_timestamp"], i["delay"])
            for i in items if i["delay"] > 0]
