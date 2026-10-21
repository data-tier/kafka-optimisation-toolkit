#!/usr/bin/env python3
"""Sample broker and controller JMX metrics from Prometheus into a CSV file.

bench/sweep.sh starts this in the background for the length of each perf run
and stops it with SIGTERM. It samples once at start, every --interval seconds
(default 5) after that, and once more when it receives SIGTERM. Each sample is a
fixed set of instant queries against Prometheus, written as one row per series
to --out with the columns

    ts,broker,metric,request_type,percentile,value

ts is Unix time in seconds (the evaluation time of the queries), broker is the
node name (broker-1..3, or controller-1..3 for the two controller MBeans),
metric is the MBean name attribute, request_type is Produce, FetchConsumer or
FetchFollower for request-time metrics and empty otherwise, and percentile is
p50, p99 or max for request-time metrics and empty otherwise.

What each metric's value is:
  * request-time metrics (TotalTimeMs and its parts): the histogram's
    50thPercentile, 99thPercentile or Max attribute in milliseconds. Kafka's
    histograms are exponentially decaying, so the percentiles weight recent
    requests. Max is the largest value since the broker started.
  * RequestHandlerAvgIdlePercent: the meter's OneMinuteRate, a fraction 0..1.
  * NetworkProcessorAvgIdlePercent, RequestQueueSize, MaxLag,
    UnderReplicatedPartitions, UnderMinIsrPartitionCount,
    ActiveControllerCount, OfflinePartitionsCount: the gauge value.
  * BytesInPerSec, BytesOutPerSec, IsrShrinksPerSec: rate(Count[30s]), the
    per-second rate over the last 30 seconds.

The sampler fails (exit status 1) as soon as any expected series is missing,
because a silent gap would make the samples look complete when they are not.
"""

import argparse
import csv
import json
import os
import signal
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

BROKERS = ["broker-1", "broker-2", "broker-3"]
CONTROLLERS = ["controller-1", "controller-2", "controller-3"]
REQUEST_TYPES = ["Produce", "FetchConsumer", "FetchFollower"]
REQUEST_MBEANS = ["TotalTimeMs", "RequestQueueTimeMs", "LocalTimeMs", "RemoteTimeMs",
                  "ResponseQueueTimeMs", "ResponseSendTimeMs"]
STATS = ["p50", "p99", "max"]
BROKER_GAUGES = ["RequestHandlerAvgIdlePercent", "NetworkProcessorAvgIdlePercent", "RequestQueueSize",
                 "MaxLag", "UnderReplicatedPartitions", "UnderMinIsrPartitionCount"]
BROKER_RATES = ["BytesInPerSec", "BytesOutPerSec", "IsrShrinksPerSec"]
CONTROLLER_GAUGES = ["ActiveControllerCount", "OfflinePartitionsCount"]

QUERIES = {
    "request": 'kot_request_time_ms{role="broker",request=~"%s"}' % "|".join(REQUEST_TYPES),
    "gauge": "kot_gauge",
    "rate": 'rate(kot_meter_count{role="broker"}[30s])',
}

HEADER = ["ts", "broker", "metric", "request_type", "percentile", "value"]

stopping = False


def on_term(signum, frame):
    global stopping
    stopping = True


def query(base_url, promql, at):
    params = urllib.parse.urlencode({"query": promql, "time": "%.3f" % at})
    url = "%s/api/v1/query?%s" % (base_url, params)
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            body = json.load(resp)
    except (urllib.error.URLError, OSError, ValueError) as e:
        raise RuntimeError("Prometheus query failed (%s): %s" % (promql, e))
    if body.get("status") != "success":
        raise RuntimeError("Prometheus query %s returned %s" % (promql, body))
    return body["data"]["result"]


def scrape(base_url, at):
    rows = []
    for r in query(base_url, QUERIES["request"], at):
        m = r["metric"]
        rows.append([m["node"], m["mbean"], m["request"], m["stat"], r["value"][1]])
    for r in query(base_url, QUERIES["gauge"], at):
        m = r["metric"]
        rows.append([m["node"], m["mbean"], "", "", r["value"][1]])
    for r in query(base_url, QUERIES["rate"], at):
        m = r["metric"]
        rows.append([m["node"], m["mbean"], "", "", r["value"][1]])
    check_complete(rows)
    return rows


def check_complete(rows):
    seen = {(r[0], r[1], r[2], r[3]) for r in rows}
    expected = set()
    for b in BROKERS:
        for mbean in REQUEST_MBEANS:
            for req in REQUEST_TYPES:
                for stat in STATS:
                    expected.add((b, mbean, req, stat))
        for g in BROKER_GAUGES + BROKER_RATES:
            expected.add((b, g, "", ""))
    for c in CONTROLLERS:
        for g in CONTROLLER_GAUGES:
            expected.add((c, g, "", ""))
    missing = sorted(expected - seen)
    if missing:
        shown = ", ".join("/".join(x for x in k if x) for k in missing[:6])
        raise RuntimeError("%d expected series missing from Prometheus, for example: %s" % (len(missing), shown))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--prometheus", required=True, help="Prometheus base URL, for example http://127.0.0.1:9090")
    ap.add_argument("--out", required=True, help="CSV file to append to (header written if new)")
    ap.add_argument("--interval", type=float, default=5.0, help="seconds between samples (default 5)")
    ap.add_argument("--ready-file", help="touched after the first successful sample")
    args = ap.parse_args()

    signal.signal(signal.SIGTERM, on_term)
    signal.signal(signal.SIGINT, on_term)

    new_file = not os.path.exists(args.out) or os.path.getsize(args.out) == 0
    with open(args.out, "a", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(HEADER)
        next_at = time.time()
        first = True
        final = False
        while True:
            # After SIGTERM take one last sample, so the samples bracket the run
            # at both ends, then stop.
            if stopping:
                final = True
            at = time.time()
            try:
                rows = scrape(args.prometheus, at)
            except RuntimeError as e:
                print("sample.py: ERROR: %s" % e, file=sys.stderr)
                return 1
            ts = "%.3f" % at
            for r in rows:
                w.writerow([ts] + r)
            f.flush()
            if first and args.ready_file:
                open(args.ready_file, "w").close()
            first = False
            if final:
                break
            next_at += args.interval
            # Sleep in short steps so SIGTERM is handled promptly.
            while not stopping and time.time() < next_at:
                time.sleep(min(0.2, max(0.0, next_at - time.time())))
    return 0


if __name__ == "__main__":
    sys.exit(main())
