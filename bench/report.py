#!/usr/bin/env python3
"""Turn sweep results into a Markdown table and a chart per sweep.

Usage: bench/report.py results [results/<sweep-dir> ...]

Each argument is either a sweep directory (one written by bench/sweep.sh) or a
directory containing sweep directories. For every sweep it writes report.md and
chart.png into that sweep's directory, and it writes REPORT.md (an index) into
every parent directory given.

Every kept producer run appears in the table on its own row. The discarded first
run of each value is left out of the table and the chart. The broker column
shows the request-time part (RequestQueueTimeMs, LocalTimeMs, RemoteTimeMs,
ResponseQueueTimeMs or ResponseSendTimeMs, at p99) whose mean changed most
between the smallest and the largest value of the setting. Values are compared
numerically when they are all numbers, otherwise the first and last values in
sweep order are used.

A directory that still carries the INCOMPLETE marker (a sweep that failed or
was interrupted) is an error, not something to skip quietly.
"""
import json
import os
import sys

import matplotlib
import matplotlib.ticker

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

ACCENT = "#2a78d6"
INK = "#0b0b0b"
INK_2 = "#52514e"
RULE = "#d9d8d4"
SURFACE = "#fcfcfb"

PARTS = ["RequestQueueTimeMs", "LocalTimeMs", "RemoteTimeMs", "ResponseQueueTimeMs", "ResponseSendTimeMs"]

# What each part of a request's time on the broker means, in plain words.
PART_MEANING = {
    ("Produce", "RequestQueueTimeMs"): "time requests waited in the broker's queue before a thread picked them up",
    ("Produce", "LocalTimeMs"): "time the leader spent writing the data to its log",
    ("Produce", "RemoteTimeMs"): "time spent waiting for the other replicas to copy the data, which is what acks=all waits for",
    ("Produce", "ResponseQueueTimeMs"): "time finished responses waited to be sent",
    ("Produce", "ResponseSendTimeMs"): "time spent sending responses back to the producer",
    ("FetchFollower", "RequestQueueTimeMs"): "time replicas' fetches waited in the leader's queue",
    ("FetchFollower", "LocalTimeMs"): "time the leader spent reading data for the replicas",
    ("FetchFollower", "RemoteTimeMs"): "time replicas' fetches waited for new data to arrive, up to 500 ms when there is none",
    ("FetchFollower", "ResponseQueueTimeMs"): "time data for the replicas waited to be sent",
    ("FetchFollower", "ResponseSendTimeMs"): "time spent sending data to the replicas",
    ("FetchConsumer", "RequestQueueTimeMs"): "time the consumer's fetches waited in the broker's queue",
    ("FetchConsumer", "LocalTimeMs"): "time the broker spent reading data for the consumer",
    ("FetchConsumer", "RemoteTimeMs"): "time the consumer's fetches waited for enough data to arrive, which fetch.min.bytes and fetch.max.wait.ms control",
    ("FetchConsumer", "ResponseQueueTimeMs"): "time data for the consumer waited to be sent",
    ("FetchConsumer", "ResponseSendTimeMs"): "time spent sending data to the consumer",
}


def fail(msg):
    sys.exit("report.py: " + msg)


def is_sweep_dir(d):
    return os.path.isfile(os.path.join(d, "sweep.json"))


def read_csv(d, name, required=True):
    p = os.path.join(d, name)
    if not os.path.isfile(p):
        if required:
            fail("%s is missing %s" % (d, name))
        return None
    if name == "broker-samples.csv":
        # Here "value" is the metric value. Everywhere else it is the setting value.
        return pd.read_csv(p, dtype={"broker": str, "metric": str, "request_type": str, "percentile": str,
                                     "value": float})
    return pd.read_csv(p, dtype={"value": str})


def ordered_values(sweep):
    return [str(v) for v in sweep["values"]]


def extremes(values):
    try:
        nums = {v: float(v) for v in values}
    except ValueError:
        return values[0], values[-1]
    return min(values, key=nums.get), max(values, key=nums.get)


def window_mean(samples, start, end, metric, request=None, stat=None, agg_brokers="mean"):
    s = samples[(samples.ts >= start) & (samples.ts <= end) & (samples.metric == metric)]
    if request is not None:
        s = s[s.request_type == request]
    if stat is not None:
        s = s[s.percentile == stat]
    s = s[s.broker.str.startswith("broker-")]
    if s.empty:
        fail("no %s samples between %.3f and %.3f" % (metric, start, end))
    per_ts = s.groupby("ts").value.agg(agg_brokers)
    return float(per_ts.mean()), int(per_ts.size)


def fmt(x, nd=0):
    if pd.isna(x):
        return "n/a"
    return f"{x:,.{nd}f}"


def biggest_part(samples, windows, values, requests):
    """windows: {value: [(start, end), ...]}. Returns (request, part, per-window means)."""
    lo, hi = extremes(values)
    best = None
    for req in requests:
        for part in PARTS:
            means = {v: [window_mean(samples, a, b, part, req, "p99")[0] for a, b in windows[v]] for v in values}
            change = abs(sum(means[hi]) / len(means[hi]) - sum(means[lo]) / len(means[lo]))
            if best is None or change > best[0]:
                best = (change, req, part, means)
    return best[1], best[2], best[3], lo, hi


def chart(path, title, values, xs_label, thr_points, thr_label, lat_points, lat_label):
    """thr_points / lat_points: {value: [y, ...]}. One accent colour for throughput, ink for latency."""
    plt.rcParams.update({"font.size": 10, "axes.edgecolor": RULE, "axes.labelcolor": INK_2,
                         "xtick.color": INK_2, "ytick.color": INK_2, "font.family": "sans-serif"})
    fig, ax = plt.subplots(figsize=(7.5, 4.2), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    x = list(range(len(values)))

    def plot(axis, points, color, marker, linestyle, label):
        means = [sum(points[v]) / len(points[v]) for v in values]
        axis.plot(x, means, color=color, linewidth=2, linestyle=linestyle, label=label, zorder=2)
        for i, v in enumerate(values):
            axis.scatter([i] * len(points[v]), points[v], s=40, color=color, marker=marker,
                         edgecolor=SURFACE, linewidth=1.5, zorder=3)

    def top(points):
        # Headroom so the highest marker is never clipped by the frame.
        return max(max(p) for p in points.values()) * 1.15 or 1

    plot(ax, thr_points, ACCENT, "o", "-", thr_label)
    ax.set_ylabel(thr_label, color=ACCENT)
    ax.set_ylim(0, top(thr_points))
    ax2 = ax.twinx()
    plot(ax2, lat_points, INK_2, "s", "--", lat_label)
    ax2.set_ylabel(lat_label, color=INK_2)
    ax2.set_ylim(0, top(lat_points))
    thousands = matplotlib.ticker.FuncFormatter(lambda y, _: f"{y:,.0f}")
    ax.yaxis.set_major_formatter(thousands)
    ax2.yaxis.set_major_formatter(thousands)
    ax.set_xlim(-0.4, len(values) - 0.6)
    for a in (ax, ax2):
        a.spines["top"].set_visible(False)
        a.tick_params(length=0)
    ax.grid(axis="y", color=RULE, linewidth=0.5)
    ax.set_axisbelow(True)
    ax.set_xticks(x, values)
    ax.set_xlabel(xs_label)
    ax.set_title(title, loc="left", color=INK, fontsize=11)
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, frameon=False, loc="upper left", bbox_to_anchor=(0, -0.16), ncol=2)
    fig.text(0.01, 0.005, "Dots are individual kept runs, lines join their means. Numbers are from this machine only.",
             color=INK_2, fontsize=7.5)
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def environment_lines(env):
    h, dk, k, disk = env["host"], env["docker"], env["kafka"], env["disk"]
    return [
        "- Computer: %s, %d cores, %.0f GB memory, %s" % (
            h["cpu_model"], h["cpu_cores_logical"], h["ram_total_bytes"] / 2**30, h["os_release"]),
        "- Docker: version %s on %s, with %d CPUs and %.1f GB memory (kernel %s)" % (
            dk["server_version"], dk["engine_os_release"], dk["engine_cpus"],
            dk["engine_ram_total_bytes"] / 2**30, dk["engine_kernel"]),
        "- Kafka: %s, Java: %s" % (k["image"], env["jvm"]["version"].splitlines()[0]),
        "- Disk: %s, %s" % (disk["model"], disk["type"]),
        "- Filesystem for Kafka's data: %s (%s)" % (env["kafka_log_dirs"]["broker-1"]["filesystem"],
                                           env["kafka_log_dirs"]["broker-1"]["mount_options"]),
    ]


def report_one(d):
    if os.path.exists(os.path.join(d, "INCOMPLETE")):
        fail("%s is incomplete (the sweep failed or was interrupted). Delete it or rerun the sweep." % d)
    sweep = json.load(open(os.path.join(d, "sweep.json")))
    env = json.load(open(os.path.join(d, "environment.json")))
    runs = read_csv(d, "runs.csv")
    consumer = read_csv(d, "consumer.csv")
    phases = read_csv(d, "phases.csv")
    samples = read_csv(d, "broker-samples.csv")
    cmetrics = read_csv(d, "client-metrics.csv")
    slow = read_csv(d, "slow-consumer.csv", required=False)
    samples["request_type"] = samples["request_type"].fillna("")
    samples["percentile"] = samples["percentile"].fillna("")

    side, setting = sweep["side"], sweep["setting"]
    values = ordered_values(sweep)
    kept = runs[runs.kept == 1]
    for v in values:
        if kept[kept.value == v].empty:
            fail("%s has no kept producer runs for %s=%s" % (d, setting, v))
        if consumer[consumer.value == v].empty:
            fail("%s has no consumer run for %s=%s" % (d, setting, v))

    def windows_for(phase, run_filter=None):
        out = {}
        for v in values:
            p = phases[(phases.value == v) & (phases.phase == phase)]
            if run_filter is not None:
                p = p[p.run.isin(run_filter[v])]
            out[v] = list(zip(p.start_ts, p.end_ts))
            if not out[v]:
                fail("%s has no %s phase timing for %s=%s" % (d, phase, setting, v))
        return out

    payload_words = {"letters": "random letters", "json": "realistic JSON records", "random-bytes": "random data"}
    lines = ["# %s (%s setting)" % (setting, side), ""]
    detail = ("Each run sent %s records of %s bytes (%s). Each value was run %d times and the first run was "
              "thrown away as a warm-up." % (f"{sweep['records']:,}", f"{sweep['record_size']:,}",
                                              payload_words.get(sweep["payload"], sweep["payload"]), sweep["runs"]))
    if sweep["fixed_producer_properties"]:
        detail += " Held the same for every value: `%s`." % " ".join(sweep["fixed_producer_properties"])
    if sweep.get("broker_update_mode"):
        how = "changed live on the running brokers" if sweep["broker_update_mode"] != "read-only" \
            else "changed by restarting the brokers"
        detail += " The setting was %s." % how
    lines += [detail, ""]
    lines += ["Command: `%s`" % sweep["command"], ""]
    lines += ["The machine (full details in `environment.json`):", ""] + environment_lines(env) + [""]

    if side in ("producer", "broker"):
        kept_runs = {v: list(kept[kept.value == v].run) for v in values}
        windows = windows_for("producer", kept_runs)
        req, part, part_means, lo, hi = biggest_part(samples, windows, values, ["Produce", "FetchFollower"])
        part_col = "Broker: %s %s, p99 (ms)" % (req, part)
        header = ["Value", "Run", "Records/s", "MB/s", "Median latency (ms)", "p99 latency (ms)",
                  "p99.9 latency (ms)", part_col]
        extra_compression = setting == "compression.type"
        if extra_compression:
            header.append("Compressed size (share of original)")
        # The broker sweep always shows replica lag and replica fetch wait, unless the
        # changed-most column above already is the replica fetch wait.
        show_fetch_wait = side == "broker" and (req, part) != ("FetchFollower", "RemoteTimeMs")
        if side == "broker":
            header.append("Replica lag, records (MaxLag)")
        if show_fetch_wait:
            header.append("Broker: replica fetch wait, p99 (ms)")
        rows = []
        for v in values:
            for i, r in enumerate(kept[kept.value == v].itertuples()):
                a, b = windows[v][i]
                row = [v, str(r.run), fmt(r.records_per_sec), fmt(r.mb_per_sec, 2), fmt(r.p50_ms), fmt(r.p99_ms),
                       fmt(r.p999_ms), fmt(part_means[v][i], 1)]
                if extra_compression:
                    m = cmetrics[(cmetrics.value == v) & (cmetrics.run == r.run) & (cmetrics.client == "producer")
                                 & (cmetrics.metric == "compression-rate-avg")]
                    row.append(fmt(float(m.metric_value.iloc[0]), 3))
                if side == "broker":
                    row.append(fmt(window_mean(samples, a, b, "MaxLag", agg_brokers="max")[0]))
                if show_fetch_wait:
                    row.append(fmt(window_mean(samples, a, b, "RemoteTimeMs", "FetchFollower", "p99")[0], 1))
                rows.append(row)
        lines += ["## Producer results", ""]
        lines += table(header, rows)
        lines += ["", "Latency is the time from sending a record to hearing back that it was stored. p99 means "
                  "99 out of 100 records were faster than this.", "",
                  "The broker column shows the part of the brokers' request handling that changed most between "
                  "%s=%s and %s=%s: %s (`%s` for %s requests). It is the 99th percentile, averaged "
                  "over the run and over the three brokers." % (
                      setting, lo, setting, hi, PART_MEANING[(req, part)], part, req), ""]
        thr = {v: list(kept[kept.value == v].records_per_sec) for v in values}
        lat = {v: list(kept[kept.value == v].p99_ms) for v in values}
        chart(os.path.join(d, "chart.png"), "%s: producer throughput and p99 latency" % setting, values, setting,
              thr, "records per second (producer)", lat, "p99 latency, ms (producer)")
    else:
        windows = windows_for("consumer")
        req, part, part_means, lo, hi = biggest_part(samples, windows, values, ["FetchConsumer"])
        header = ["Value", "Records/s", "MB/s", "MB/s while fetching", "Group join (ms)", "Average fetch (ms)",
                  "Broker: fetch time, p99 (ms)", "Broker: %s %s, p99 (ms)" % (req, part)]
        rows, thr, lat = [], {}, {}
        for v in values:
            c = consumer[consumer.value == v].iloc[0]
            a, b = windows[v][0]
            total_p99 = window_mean(samples, a, b, "TotalTimeMs", "FetchConsumer", "p99")[0]
            fl = cmetrics[(cmetrics.value == v) & (cmetrics.client == "consumer") & (cmetrics.metric == "fetch-latency-avg")]
            rows.append([v, fmt(c.records_per_sec), fmt(c.mb_per_sec, 2), fmt(c.fetch_mb_per_sec, 2),
                         fmt(c.rebalance_time_ms), fmt(float(fl.metric_value.iloc[0]), 1), fmt(total_p99, 1),
                         fmt(part_means[v][0], 1)])
            thr[v] = [float(c.records_per_sec)]
            lat[v] = [total_p99]
        lines += ["## Consumer results", ""]
        lines += ["The consumer test runs once per value and does not measure latency itself, so the latency "
                  "columns come from the brokers: how long they took to answer the consumer's fetch requests.", ""]
        lines += table(header, rows)
        lines += ["", "The last column shows the part of the brokers' fetch handling that changed most between "
                  "%s=%s and %s=%s: %s (`%s`)." % (
                      setting, lo, setting, hi, PART_MEANING[(req, part)], part), ""]
        chart(os.path.join(d, "chart.png"), "%s: consumer throughput and broker fetch p99" % setting, values, setting,
              thr, "records per second (consumer)", lat, "p99 time to answer a fetch, ms (broker)")
        if slow is not None:
            lines += ["## Slow consumer (spends 1 ms on each record)", ""]
            srows = []
            for v in values:
                s = slow[slow.value == v].iloc[0]
                srows.append([v, fmt(s.records), fmt(s.records_per_sec, 1), fmt(s.polls), fmt(s.poll_idle_ratio_avg, 3),
                              fmt(s.records_lag_max), fmt(s.app_backlog_records), fmt(s.producer_rate)])
            lines += table(["Value", "Records handled", "Records/s", "Polls", "Time idle in poll",
                            "records-lag-max", "Records waiting at the end", "Producer records/s"], srows)
            lines += ["", "Time idle in poll (`poll-idle-ratio-avg`) is the share of time the consumer spent waiting "
                      "for data rather than working: near 0 means it was busy all the time. `records-lag-max` only "
                      "counts records not yet downloaded, so it can read 0 even when the application is behind. "
                      "Records waiting at the end counts everything the application had not yet handled when the "
                      "run stopped.", ""]

    lines += ["![%s chart](chart.png)" % setting, ""]
    lines += ["The complete output of every run is in `raw/`. These numbers describe this machine only. "
              "Read \"How to read the numbers\" in the README before comparing them with anything.", ""]
    with open(os.path.join(d, "report.md"), "w") as f:
        f.write("\n".join(lines))
    return "%s (%s side)" % (setting, side)


def table(header, rows):
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" if i < 1 else "---:" for i in range(len(header))) + "|"]
    for r in rows:
        out.append("| " + " | ".join(r) + " |")
    return out


def main(argv):
    if not argv:
        fail("usage: bench/report.py results [results/<sweep-dir> ...]")
    for arg in argv:
        if not os.path.isdir(arg):
            fail("%s is not a directory" % arg)
        if is_sweep_dir(arg):
            print("report.py: %s -> %s" % (report_one(arg), os.path.join(arg, "report.md")))
            continue
        subdirs = sorted(os.path.join(arg, e) for e in os.listdir(arg) if is_sweep_dir(os.path.join(arg, e)))
        if not subdirs:
            fail("no sweep results in %s. Run a sweep first, for example make sweep-linger." % arg)
        index = ["# Sweep reports", "", "Generated by bench/report.py from the directories below.", ""]
        for d in subdirs:
            what = report_one(d)
            print("report.py: %s -> %s" % (what, os.path.join(d, "report.md")))
            index.append("- [%s](%s/report.md): %s" % (os.path.basename(d), os.path.basename(d), what))
        with open(os.path.join(arg, "REPORT.md"), "w") as f:
            f.write("\n".join(index) + "\n")
        print("report.py: index -> %s" % os.path.join(arg, "REPORT.md"))


if __name__ == "__main__":
    main(sys.argv[1:])
