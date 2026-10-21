# ZooKeeper to KRaft migration lab

> **For throwaway clusters only.** These scripts delete data, rewrite configuration files and finish a migration that cannot be undone. Never point them at a cluster you care about.

A practice run of moving a Kafka cluster from ZooKeeper to KRaft, following the Apache Kafka 3.9 documentation step by step: https://kafka.apache.org/39/operations/kraft/#zookeeper-to-kraft-migration. Each step is a script that says what it is about to do, does it, then shows the proof the documentation tells you to look for. `transcript.sh` runs the whole thing, including a rollback, and saves everything it printed.

Everything runs in Docker: three ZooKeeper nodes, three Kafka 3.9.1 brokers (numbered 1, 2 and 3), three KRaft controllers (numbered 3000, 3001 and 3002), and a container for running Kafka's command-line tools. Nothing is exposed on your machine. The lab is separate from the benchmark cluster in the rest of this repository, and uses Kafka 3.9 because Kafka 4 cannot run with ZooKeeper at all.

## Prerequisites

- Docker with the Compose plugin, and about 6 GB of memory for Docker.
- bash. Keep the repository somewhere Docker can see (on a Mac with Docker Desktop or Colima, anywhere in your home folder).
- An internet connection the first time, to download the Kafka and ZooKeeper images.

## How long it takes

About 12 minutes for `transcript.sh` on a laptop. Most of that is restarting brokers one at a time and waiting for each to catch up before the next. Each step on its own takes 1 to 3 minutes.

## Order

Run from this directory:

```sh
./reset.sh                          # start from nothing (deletes everything the lab created)
./00-seed.sh                        # start a ZooKeeper-based cluster and give it some data
./01-provision-controllers.sh       # set up and start the three KRaft controllers
./02-brokers-migration-mode.sh      # restart the brokers in migration mode and wait for the metadata to copy over
./03-brokers-to-kraft.sh            # restart the brokers as KRaft brokers
./04-finalize.sh                    # cut ZooKeeper out for good (cannot be undone)
```

`./verify.sh` can be run after any step. It checks that nothing was lost or changed: both topics still hold exactly the 1000 records each that were written at the start, and the consumer group `lab-orders` still remembers how far it had read. It prints PASS or FAIL.

To rehearse a rollback, stop after step 02 or 03 and run the matching script:

```sh
./rollback-from-step-2.sh    # after step 02: back to ZooKeeper, restarting each broker once
./rollback-from-step-3.sh    # after step 03: back to ZooKeeper, restarting each broker twice
```

Both finish by proving the cluster is running on ZooKeeper alone again, with no data lost. From there you can start the migration again at `01-provision-controllers.sh`.

To produce the transcript for the book:

```sh
./transcript.sh    # migrate, roll back, then migrate again and finish, checking the data after every step
```

It saves everything to `transcript-<date>.log` in this folder, and will not overwrite an earlier one.

## The order is enforced

The lab remembers which step finished last, and each script refuses to run unless the step before it has finished:

| Script | Runs after |
|---|---|
| `00-seed.sh` | nothing (a fresh `reset.sh`) |
| `01-provision-controllers.sh` | `00-seed.sh`, or either rollback |
| `02-brokers-migration-mode.sh` | `01-provision-controllers.sh` |
| `03-brokers-to-kraft.sh` | `02-brokers-migration-mode.sh` |
| `04-finalize.sh` | `03-brokers-to-kraft.sh` |
| `rollback-from-step-2.sh` | `02-brokers-migration-mode.sh` |
| `rollback-from-step-3.sh` | `03-brokers-to-kraft.sh` |

If a step fails part of the way through, every script refuses to run until you have looked at what went wrong and started again with `./reset.sh`.

## Where the lab follows the documentation rather than habit

- It uses Kafka **3.9.1**, because the documentation says brokers must be on 3.9.1 before migrating.
- The controllers are set up the way the documentation recommends: controller 3000 starts on its own, then 3001 and 3002 are started and added to it one at a time with `kafka-metadata-quorum.sh add-controller`.
- Both rollbacks include a safety check the documentation added recently (for bug KAFKA-20488): making sure ZooKeeper's controller counter is ahead of the one KRaft used, so the old-style controller can take over cleanly.
- The documentation quotes the "migration completed" log line as "Zookeeper", but Kafka actually writes "ZooKeeper". The lab accepts either.

Where each of these comes from, and every setting the lab uses, is listed in [SOURCES.md](SOURCES.md). Every configuration file a node runs with is written by `lib.sh`, and every change to one is printed before the node restarts.

## Files

| File | Purpose |
|---|---|
| `docker-compose.yml` | The ZooKeeper nodes, brokers and controllers (the controllers only start in step 01), and the tools container |
| `start-node.sh` | Starts one Kafka node on its current config file |
| `lib.sh` | Shared code: writes the config files, enforces the step order, talks to ZooKeeper and the controllers, restarts brokers one at a time |
| `reset.sh` | Deletes everything the lab created |
| `00-seed.sh` to `04-finalize.sh` | The migration steps |
| `rollback-from-step-2.sh`, `rollback-from-step-3.sh` | The two ways back to ZooKeeper |
| `verify.sh` | Checks no data or consumer progress was lost |
| `transcript.sh` | The full run, saved to `transcript-<date>.log` |
| `run/` | Working files created while the lab runs (not committed) |
