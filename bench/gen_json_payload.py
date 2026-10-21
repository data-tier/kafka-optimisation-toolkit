#!/usr/bin/env python3
"""Write a file of JSON-like records, one per line, for --payload-file.

kafka-producer-perf-test reads the file as UTF-8 text, splits it on newlines
and picks a line at random for every record it sends (ProducerPerformance
readPayloadFile and generateRandomPayload in Kafka 4.3). The records look like
typical application events: repeated field names, a small vocabulary of
values, timestamps and numbers, padded with an items array to roughly
--record-size bytes. The seed is fixed so every reader gets the same file.

Usage: bench/gen_json_payload.py --record-size 1024 --lines 10000 --out FILE
"""
import argparse
import json
import random

EVENTS = ["page_view", "add_to_cart", "checkout", "search", "login", "logout", "refund"]
COUNTRIES = ["GB", "US", "DE", "FR", "IN", "BR", "JP", "NG", "AU", "CA"]
DEVICES = ["ios", "android", "web", "desktop"]
SKUS = ["SKU-%05d" % i for i in range(500)]


def record(rng, i, target):
    rec = {
        "event_id": "%016x" % rng.getrandbits(64),
        "seq": i,
        "type": rng.choice(EVENTS),
        "ts": "2026-%02d-%02dT%02d:%02d:%02d.%03dZ" % (
            rng.randint(1, 12), rng.randint(1, 28), rng.randint(0, 23),
            rng.randint(0, 59), rng.randint(0, 59), rng.randint(0, 999)),
        "user": {"id": "u%07d" % rng.randint(0, 2_000_000), "country": rng.choice(COUNTRIES),
                 "device": rng.choice(DEVICES), "logged_in": rng.random() < 0.7},
        "session": "%08x" % rng.getrandbits(32),
        "items": [],
    }
    while True:
        item = {"sku": rng.choice(SKUS), "qty": rng.randint(1, 5),
                "price_pence": rng.randint(99, 99_999), "currency": "GBP"}
        candidate = json.dumps(dict(rec, items=rec["items"] + [item]), separators=(",", ":"))
        if len(candidate) > target and rec["items"]:
            break
        rec["items"].append(item)
    return json.dumps(rec, separators=(",", ":"))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--record-size", type=int, required=True, help="approximate bytes per record")
    ap.add_argument("--lines", type=int, default=10000, help="number of distinct records (default 10000)")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    if args.record_size < 200:
        ap.error("--record-size must be at least 200 bytes for a JSON-like record")
    rng = random.Random(20261021)
    with open(args.out, "w", encoding="utf-8") as f:
        for i in range(args.lines):
            line = record(rng, i, args.record_size)
            assert "\n" not in line
            f.write(line + "\n")


if __name__ == "__main__":
    main()
