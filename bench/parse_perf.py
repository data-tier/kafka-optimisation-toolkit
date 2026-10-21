#!/usr/bin/env python3
"""Parse the output of the Kafka perf tools into CSV fields for bench/sweep.sh.

Each subcommand reads one saved tool log and prints one CSV line (no header)
on stdout, or exits non-zero with a message saying what is wrong. A run that
did not finish cleanly must never become a row.

  producer LOG --expect-records N
      The final summary line of kafka-producer-perf-test (or the bundled
      RandomBytesProducerPerformance, which prints the same line). Format from
      ProducerPerformance.Stats.printTotal in Kafka 4.3:
      "%d records sent, %f records/sec (%.2f MB/sec), %.2f ms avg latency,
       %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th."
      Prints records_per_sec,mb_per_sec,avg_ms,max_ms,p50_ms,p95_ms,p99_ms,p999_ms.

  consumer LOG --expect-records N
      The line after the header of kafka-consumer-perf-test. Format from
      ConsumerPerformance.run in Kafka 4.3 (ten comma-separated fields).
      Prints start_time,end_time,data_consumed_mb,mb_per_sec,records_consumed,
      records_per_sec,rebalance_time_ms,fetch_time_ms,fetch_mb_per_sec,
      fetch_records_per_sec.

  client-metrics LOG --client producer|consumer
      The --print-metrics table (ToolsUtils.printMetrics in Kafka 4.3), keeping
      the client-level metrics listed in METRICS. Prints one metric,value line
      per metric. For a producer, fails if record-error-total is not zero.

  slow LOG
      The SUMMARY line of the bundled SlowConsumer.
      Prints records,elapsed_ms,records_per_sec,polls,poll_idle_ratio_avg,records_lag_max,
      app_backlog_records.
"""
import argparse
import math
import re
import sys

PRODUCER_RE = re.compile(
    r"^(\d+) records sent, ([0-9.]+) records/sec \(([0-9.]+) MB/sec\), ([0-9.]+) ms avg latency, "
    r"([0-9.]+) ms max latency, (\d+) ms 50th, (\d+) ms 95th, (\d+) ms 99th, (\d+) ms 99\.9th\.$")

CONSUMER_HEADER = ("start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, "
                   "rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec")

METRIC_RE = re.compile(r"^([\w.-]+):([\w.-]+):\{([^}]*)\}\s*:\s*(\S+)\s*$")

METRICS = {
    "producer": {
        "producer-metrics": ["compression-rate-avg", "batch-size-avg", "records-per-request-avg",
                             "request-latency-avg", "record-queue-time-avg", "bufferpool-wait-ratio",
                             "record-error-total", "record-retry-total"],
    },
    "consumer": {
        "consumer-fetch-manager-metrics": ["fetch-latency-avg", "fetch-size-avg", "records-per-request-avg",
                                           "fetch-rate", "records-lag-max"],
        "consumer-metrics": ["poll-idle-ratio-avg"],
    },
}


def fail(msg):
    sys.exit("parse_perf.py: " + msg)


def number(s, what):
    try:
        x = float(s)
    except ValueError:
        fail("%s is not a number: %r" % (what, s))
    if math.isnan(x) or math.isinf(x):
        fail("%s is %s" % (what, s))
    return s


def producer(lines, expect):
    hits = [m for m in (PRODUCER_RE.match(l.strip()) for l in lines) if m]
    if len(hits) != 1:
        fail("expected exactly one producer summary line, found %d" % len(hits))
    g = hits[0].groups()
    if int(g[0]) != expect:
        fail("producer sent %s records, expected %d (failed sends are not counted, see the log)" % (g[0], expect))
    names = ["records_per_sec", "mb_per_sec", "avg_ms", "max_ms", "p50_ms", "p95_ms", "p99_ms", "p999_ms"]
    print(",".join(number(v, n) for v, n in zip(g[1:], names)))


def consumer(lines, expect):
    for l in lines:
        if l.startswith("WARNING: Exiting before consuming the expected number of records"):
            fail("consumer timed out before reading %d records: %s" % (expect, l.strip()))
    idx = [i for i, l in enumerate(lines) if l.strip() == CONSUMER_HEADER]
    if len(idx) != 1 or idx[0] + 1 >= len(lines):
        fail("consumer header line not found exactly once")
    fields = [f.strip() for f in lines[idx[0] + 1].split(",")]
    if len(fields) != 10:
        fail("consumer result line has %d fields, expected 10: %r" % (len(fields), lines[idx[0] + 1]))
    if int(fields[4]) < expect:
        fail("consumer read %s records, expected %d" % (fields[4], expect))
    for i, n in enumerate(["data_consumed_mb", "mb_per_sec", "records_consumed", "records_per_sec",
                           "rebalance_time_ms", "fetch_time_ms", "fetch_mb_per_sec", "fetch_records_per_sec"]):
        number(fields[i + 2], n)
    print(",".join(fields))


def client_metrics(lines, client):
    wanted = METRICS[client]
    found = {}
    for l in lines:
        m = METRIC_RE.match(l.strip())
        if not m:
            continue
        group, name, tags, value = m.groups()
        # Client-level metrics carry only the client-id tag.
        if group in wanted and name in wanted[group] and re.fullmatch(r"client-id=[^,]+", tags):
            found[name] = value
    missing = [n for names in wanted.values() for n in names if n not in found]
    if missing:
        fail("metrics missing from --print-metrics output: %s" % ", ".join(missing))
    if client == "producer" and float(found["record-error-total"]) != 0:
        fail("producer reported record-error-total=%s" % found["record-error-total"])
    for names in wanted.values():
        for n in names:
            print("%s,%s" % (n, found[n]))


def slow(lines):
    hits = [l for l in lines if l.startswith("SUMMARY ")]
    if len(hits) != 1:
        fail("expected one SUMMARY line from SlowConsumer, found %d" % len(hits))
    kv = dict(p.split("=", 1) for p in hits[0].split()[1:])
    names = ["records", "elapsed_ms", "records_per_sec", "polls", "poll_idle_ratio_avg", "records_lag_max",
             "app_backlog_records"]
    if int(kv["records"]) <= 0:
        fail("SlowConsumer consumed no records")
    print(",".join(number(kv[n], n) for n in names))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("kind", choices=["producer", "consumer", "client-metrics", "slow"])
    ap.add_argument("log")
    ap.add_argument("--expect-records", type=int)
    ap.add_argument("--client", choices=["producer", "consumer"])
    a = ap.parse_args()
    with open(a.log, encoding="utf-8", errors="replace") as f:
        lines = f.read().splitlines()
    if a.kind in ("producer", "consumer") and a.expect_records is None:
        ap.error("--expect-records is required for %s" % a.kind)
    if a.kind == "producer":
        producer(lines, a.expect_records)
    elif a.kind == "consumer":
        consumer(lines, a.expect_records)
    elif a.kind == "client-metrics":
        if not a.client:
            ap.error("--client is required for client-metrics")
        client_metrics(lines, a.client)
    else:
        slow(lines)


if __name__ == "__main__":
    main()
