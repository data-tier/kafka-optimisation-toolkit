# Kafka Optimisation Toolkit

This is the companion to *Kafka Optimisation: a quick-start guide to brokers, producers and consumers, tuned from first principles*. The book's appendix points you here.

The book explains how producer, consumer and broker settings affect Kafka's throughput and latency, and shows benchmark tables to back that up. This toolkit lets you run the same benchmarks on your own machine, so you can see the effects for yourself rather than take the book's word for it.

This guide takes you from a fresh download to your first set of results, then shows you how to read them. You do not need to know Docker or Kafka's command-line tools to follow it.

> The repository contains no benchmark results. Every number you will see comes from your own machine.

**Already set up and in a hurry?** The whole thing in one line:

```sh
make cluster-up && make sweep-linger && make report
```

## What you will do

1. Check you have the tools you need
2. Download the toolkit
3. Start a small Kafka cluster on your machine
4. Check the cluster is healthy
5. Try a quick test run
6. Run your first real sweep
7. Read the report
8. Run the rest of the book's sweeps
9. Shut everything down

Along the way, the guide explains what is happening behind the scenes. If something goes wrong, see [When something goes wrong](#when-something-goes-wrong) near the end.

---

## Step 1: Check you have the tools you need

You need four things:

| Tool | Version | How to check |
|---|---|---|
| Docker, with the Compose plugin | Docker Desktop, Colima or Docker on Linux | `docker compose version` |
| make | any | `make --version` |
| bash | any, including the one built into macOS | `bash --version` |
| Python | 3.10 or newer | `python3 --version` |

Then make sure Docker has enough room to work:

- **Memory: 12 GB or more for Docker.** The cluster runs six Kafka processes at 1 GB each, and Kafka works best with plenty of memory left over for the operating system to cache files. On Docker Desktop, set this under Settings, Resources. On Colima, stop it with `colima stop`, then start it again with `colima start --memory 12 --cpu 4`.
- **CPUs: 4 or more for Docker.**
- **Disk: at least 12 GB free.** With the default settings, each value in a sweep writes about 3 GB of data (three runs of a million 1 KB records), and the cluster keeps three copies of it, so about 9 GB sits on disk until the sweep moves on to the next value.

You also need an internet connection the first time, to download the Kafka, Java and Prometheus images.

The toolkit runs with less memory and fewer CPUs, and will warn you when it starts. The results will then say more about your machine running short of resources than about Kafka.

## Step 2: Download the toolkit

```sh
git clone https://github.com/data-tier/kafka-optimisation-toolkit.git
cd kafka-optimisation-toolkit
```

**On a Mac, clone it somewhere inside your home folder.** Docker Desktop and Colima can only see folders they share with Docker, and by default that is your home folder. If you clone it elsewhere, Step 3 will stop with a message telling you so.

## Step 3: Start the cluster

```sh
make cluster-up
```

The first time, this downloads the images and builds the toolkit's own image, which can take several minutes. After that it starts in under a minute.

**What is happening:** Docker starts eight containers:

- **three controllers**, which keep track of the cluster's metadata (which topics exist, which broker leads which partition)
- **three brokers**, which store the data and serve producers and consumers
- **a tools container**, where all the tests run, kept separate from the brokers
- **Prometheus**, which collects metrics from every broker and controller every 5 seconds

Each node runs Kafka 4.3.0 with its default settings. The only things set by hand are the ones a node cannot start without: its name, its role, how to reach the other nodes, and where to store its data. You can read each node's settings in `cluster/config/`, with a comment explaining every line.

When it is ready, the last line looks like this:

```
cluster ready: 3 controllers voting, 3 brokers registered, 6/6 exporter targets up
```

If you see a warning about memory or CPUs above that line, see [Step 1](#step-1-check-you-have-the-tools-you-need).

## Step 4: Check the cluster is healthy

```sh
make cluster-health
```

This asks the cluster two questions: are the controllers agreeing with each other, and is every partition fully copied? The output looks like this (an example from one machine, your IDs will differ):

```
== kafka-metadata-quorum.sh describe --status
ClusterId:              92o3TbznTViYOiLwNLN65A
LeaderId:               101
LeaderEpoch:            1
HighWatermark:          45
MaxFollowerLag:         0
MaxFollowerLagTimeMs:   0
CurrentVoters:          [{"id": 101, ...}, {"id": 102, ...}, {"id": 103, ...}]
CurrentObservers:       [{"id": 1, ...}, {"id": 3, ...}, {"id": 2, ...}]

== Under-replicated partitions (kafka-topics.sh --describe --under-replicated-partitions)
under-replicated partitions: 0

== UnderReplicatedPartitions per broker (JMX via Prometheus)
broker-1: 0
broker-2: 0
broker-3: 0
```

**What to look for:**

- `LeaderId` is one of the controllers (101, 102 or 103). That controller is currently in charge.
- `CurrentVoters` lists all three controllers, and `CurrentObservers` lists all three brokers (1, 2 and 3).
- `under-replicated partitions: 0`. Every partition has all of its copies.

If any of these are off, the command fails with a message saying what is wrong.

## Step 5: Try a quick test run

Before committing to a full-size run, try a small one to check everything works end to end:

```sh
make sweep-linger RECORDS=20000 RUNS=2
make report
```

This takes a few minutes, a little longer the first time while `make report` installs what it needs. It sends 20,000 records per run instead of a million, which is far too few for meaningful numbers but enough to prove the setup works. When it finishes, open `results/REPORT.md` and follow the link to the linger.ms report. If it has a table and a chart, you are ready.

Delete the test results before going on, so they do not mix with real ones:

```sh
rm -rf results/2* results/REPORT.md
```

## Step 6: Run your first real sweep

```sh
make sweep-linger
```

A **sweep** tests one setting at several values, one value at a time. This one tests the producer's `linger.ms`, how long the producer waits to fill a batch before sending it, at 0, 5, 20, 50 and 100 milliseconds.

**What happens for each value:**

1. A new, empty topic is created, with 12 partitions copied to all three brokers.
2. Kafka's own producer test tool sends a million 1 KB records as fast as it can, with the setting at that value. It does this three times.
3. The first of the three runs is thrown away as a warm-up. [How to read the numbers](#how-to-read-the-numbers) explains why.
4. Kafka's consumer test tool reads the records back once.
5. Throughout, the brokers' own metrics are recorded every 5 seconds: how long requests took, how busy the brokers were, how much data flowed in and out.
6. The topic is deleted, and the next value starts.

The sweep prints what it is doing as it goes, including the result of each run. How long it takes depends on your machine: it scales with the number of records, and a full-size sweep takes much longer than the quick test in Step 5.

**If a run fails, the sweep stops.** It will not carry on and give you a report built on partial data. The results folder is marked `INCOMPLETE`, and the report step refuses to use it. Fix the cause, delete that folder, and run the sweep again.

## Step 7: Read the report

```sh
make report
```

The first time, this installs two Python libraries (`pandas` and `matplotlib`) into a `.venv` folder inside the toolkit. It does not touch the rest of your system.

Open `results/REPORT.md`. It links to one report per sweep. Each report has:

**A description of the run and of your machine.** The settings used, and your CPU, memory, disk, Docker and Kafka versions. Anyone reading your results needs these to make sense of them.

**A results table**, one row per kept run:

| Column | What it means |
|---|---|
| Value | The value of the setting being tested |
| Run | Which run this was (run 1 is always the thrown-away warm-up, so you will see 2 and 3) |
| Records/s and MB/s | Throughput: how much data the producer got through per second |
| Median latency | Half of all records were stored faster than this |
| p99 latency | 99 out of 100 records were stored faster than this. The slow 1% is often what users notice |
| p99.9 latency | 999 out of 1000 records were stored faster than this |
| Broker column | The part of the brokers' work that changed most across the sweep, for example time spent waiting for replicas to copy the data. The report explains in words what it is |

Latency here means the time from the producer sending a record to the broker confirming it was safely stored.

**A chart** of throughput (blue, left axis) and p99 latency (grey, right axis) against the setting. Dots are individual runs, lines join their averages. The shape of these two lines is usually the most useful thing in the report: where throughput stops improving, and where latency starts to climb.

## How to read the numbers

Three things to understand before you trust, compare or quote any of the results.

**Why the first run is thrown away.** The first run of each value is a warm-up. It writes to a brand-new topic, Java has not yet optimised the test tool's code, the producer is still finding out which broker leads which partition, and the brokers' figures still partly reflect the previous value. It measures start-up, not the setting. The toolkit keeps it in `runs.csv` (marked `kept=0`) so you can see how big the effect is, but leaves it out of the table and chart.

**Why both kept runs are shown.** With three runs, two are kept, and the table shows each one rather than an average. Two runs are not enough for proper statistics, but they show at a glance how much the result wobbles from one run to the next. If two runs of the same value differ by more than two neighbouring values do, the sweep has not shown a real difference. Run it again with `RUNS=5`, or with more records, before drawing a conclusion.

**Why you should not compare numbers between machines.** Throughput and latency depend on the CPU and how hot it is running, the memory, the disk, the operating system, whether Docker runs inside a virtual machine, the Java version, and whatever else the machine is doing at the time. On a laptop, all six Kafka nodes and the test tools share the same processor and disk, so the producer and the brokers compete for CPU in a way they never would in production. That is why every results folder includes a description of the machine. Compare values within one sweep, on one machine. The shape of a curve (where throughput levels off, where latency starts to climb) carries over between machines far better than any single number, and the shape is what the book discusses.

A few more things that can catch you out:

- **Broker latency figures lag behind.** Kafka's request-time percentiles favour recent requests but reflect roughly the last minute or two, so the start of a run can still include the end of the previous one. The broker's "max" is the largest value since the broker started, not just during the run. Very short runs give too few samples to mean much.
- **Replication requests show about 500 ms of waiting when there is nothing to copy.** Brokers asking for new data wait up to half a second for some to arrive (`replica.fetch.wait.max.ms`). The figure drops as soon as there is data to replicate.
- **`acks=1` changes two things at once.** It also quietly switches off the producer's duplicate protection (idempotence), so the `acks` sweep measures both changes together.
- **`records-lag-max` does not show a slow application falling behind.** It only counts records the consumer has not yet downloaded. Records already downloaded but still waiting to be processed are not included, so a slow application can be far behind while this reads zero. `sweep-poll` therefore also reports the real backlog: how many records are waiting that the application has not handled yet.
- **A new consumer group waits about 3 seconds before it starts** (`group.initial.rebalance.delay.ms`). The consumer test counts this in its total time and also reports it separately.

## Step 8: Run the rest of the book's sweeps

There are seven sweeps in all, matching the book's benchmark tables:

| Command | Setting tested | Values |
|---|---|---|
| `make sweep-linger` | Producer `linger.ms` | 0, 5, 20, 50 and 100 ms |
| `make sweep-batch` | Producer `batch.size` | 16 KB, 64 KB, 256 KB and 1 MB, with `linger.ms` fixed at 20 ms |
| `make sweep-compression` | Producer `compression.type` | none, lz4, snappy, zstd and gzip. Runs twice: once with realistic JSON records and once with random data that cannot be compressed |
| `make sweep-acks` | Producer `acks` | 1 and all |
| `make sweep-fetch-min` | Consumer `fetch.min.bytes` | 1 byte, 64 KB and 1 MB |
| `make sweep-fetchers` | Broker `num.replica.fetchers` | 1, 2 and 4, while a producer sends as fast as it can |
| `make sweep-poll` | Consumer `max.poll.records` | 100, 500 and 2000, including a consumer that spends 1 ms on each record, like a real application doing work |

Run them one at a time, or all seven in order with:

```sh
make sweep-all
make report
```

`make sweep-all` takes a long time. It is a good thing to start before you go to bed.

## Step 9: Shut everything down

When you are finished:

```sh
make cluster-down     # stop the cluster, keep its data for next time
make cluster-reset    # stop the cluster and delete all of its data
```

Your results stay in `results/` either way.

---

## Going further

### Change the size of a sweep

Every sweep takes these options:

| Option | Default | Meaning |
|---|---|---|
| `RECORDS=` | 1000000 | Records sent per run |
| `RECORD_SIZE=` | 1024 | Bytes per record |
| `RUNS=` | 3 | Runs per value, including the warm-up |

For example, `make sweep-linger RUNS=5 RECORDS=2000000`. `make sweep-poll` also takes `SLOW_RATE=` (records per second sent while the slow consumer runs, default 800) and `SLOW_DURATION=` (how many seconds it runs, default 60).

### Test any other setting

The `make` targets are shortcuts for `bench/sweep.sh`, which can test any producer, consumer or broker setting:

```sh
bench/sweep.sh <producer|consumer|broker> <setting> <value1,value2,...> [options]
```

For example, to test the producer's `buffer.memory`:

```sh
bench/sweep.sh producer buffer.memory 16777216,33554432,67108864
```

Run `bench/sweep.sh --help` for all the options, including `--fixed` to hold another setting constant and `--payload` to choose what each record contains.

For a broker setting, the toolkit checks whether Kafka can change it while running. If it can, it is changed live. If not, the brokers are restarted with the new value. Either way, the log says which happened, and every broker is checked before the test starts.

### What each record contains

| `--payload` | What each record contains | How well it compresses |
|---|---|---|
| `letters` (default) | Random capital letters A to Z, which is what Kafka's own test tool sends | Somewhat. With only 26 possible characters, it can shrink to about 60% of its size at best |
| `json` | Realistic JSON events (orders, page views and so on), picked at random from 10,000 generated by `bench/gen_json_payload.py` | Very well, like real application data |
| `random-bytes` | Completely random data, sent by a small add-on to Kafka's test tool | Not at all |

Kafka's own test tool cannot send random data: it only produces letters or lines of text. The add-on is Kafka's tool unchanged except for the contents of each record, so it measures and reports in exactly the same way and its numbers can be compared directly.

---

## When something goes wrong

| What you see | What it means and what to do |
|---|---|
| `Docker cannot see files in ...` | Docker cannot reach the folder you cloned into. On a Mac, move the toolkit into your home folder, or add its folder to Docker's file sharing settings |
| `WARNING: Docker has 7.7 GB of memory ...` | Docker has less memory than recommended. The toolkit still runs, but give Docker more memory (Step 1) before trusting the results |
| `cluster not ready after 180s` | One of the nodes did not start. `docker compose -f cluster/docker-compose.yml logs` shows why. Low memory is the most common cause |
| `the Docker daemon is not reachable` | Docker is not running. Start Docker Desktop or Colima |
| `port is already allocated` when starting the cluster | Something else on your machine is using port 9090. Pick another port and set it for every command, for example `export KOT_PROMETHEUS_PORT=9091` before running `make cluster-up` and the sweeps |
| `SWEEP FAILED ... is incomplete` | A run failed part of the way through. The message above it says why. Delete that results folder and run the sweep again |
| `report.py: ... is incomplete` | The report found a sweep that did not finish. Delete that folder from `results/`, then run `make report` again |
| `--runs must be at least 2` | The first run is always thrown away, so at least one more is needed |

---

## Reference

### What is in a results folder

Each sweep writes a folder named after the time it started and the setting it tested, for example `results/20261021T203243Z-producer-linger.ms/`. Inside it:

| File | Contents |
|---|---|
| `report.md`, `chart.png` | The report and chart, written by `make report` |
| `runs.csv` | One row per producer run: throughput (records and MB per second) and latency (average, maximum, and the 50th, 95th, 99th and 99.9th percentiles). The `kept` column is 0 for the first run, which is thrown away |
| `consumer.csv` | One row per value from the consumer test: how much it read, how fast, and how long it spent joining its consumer group |
| `broker-samples.csv` | What each broker reported every 5 seconds during the runs: how long produce and fetch requests took and where that time went, how busy its threads were, data in and out per second, how far its replicas lagged, and whether any partitions were missing a replica |
| `phases.csv` | When each run started and ended, so the broker samples can be matched to the run they belong to |
| `client-metrics.csv` | The producer's and consumer's own metrics for every run, such as how well data compressed, how big the batches were, and how long fetches took |
| `slow-consumer.csv` | `sweep-poll` only: how fast the slow consumer got through records, and how far behind it fell |
| `environment.json` | A description of the machine: CPU, memory, disk, operating system, Docker, the exact Kafka and Java versions and settings each node was running with. If any of it cannot be found out, the sweep stops rather than leave a gap |
| `sweep.json` | The exact command and settings used |
| `raw/` | The complete, unedited output of every test run |

In `broker-samples.csv`, request times are in milliseconds, idle figures are fractions from 0 (always busy) to 1 (always idle), and per-second figures are averaged over the previous 30 seconds.

### Things that work differently from what you might expect

- **The controllers report metrics too.** Two of the metrics the book uses (`ActiveControllerCount` and `OfflinePartitionsCount`) only exist on controllers, never on brokers, so the controllers are monitored as well.
- **Kafka is started directly, not through the image's start-up script.** The official Docker image quietly changes some Kafka defaults when it starts. Starting Kafka directly on the config files in `cluster/config/` means every setting not listed there really is Kafka's default.
- **A few consumer settings cannot be tested.** Kafka's consumer test tool overrides some settings with its own values, whatever you pass in (for example `max.partition.fetch.bytes`). `sweep.sh` refuses to sweep those rather than silently test the tool's value instead of yours.
- **Every producer run also prints its metrics.** Asking the producer test for metrics slightly changes how it finishes a run, so the toolkit always asks, which keeps every run measured the same way.

Where each of these facts comes from, down to the line of Kafka's source code, is listed in [SOURCES.md](SOURCES.md).

### How the repository is laid out

```
cluster/        the Kafka cluster: Docker setup, each node's config, metrics collection
bench/          the sweep, sampling, environment and report scripts, and the small Java add-ons
migration-lab/  a separate, scripted ZooKeeper-to-KRaft migration rehearsal (see its README)
results/        created when you run sweeps, never committed
work/           temporary files shared with the test container, never committed
```

### Also in this repository

**The migration lab** in `migration-lab/` is a separate, step-by-step practice run of moving a Kafka cluster from ZooKeeper to KRaft, following the Kafka 3.9 documentation. It has its own [guide](migration-lab/README.md).

## Licence

Apache License 2.0. See [LICENSE](LICENSE).
