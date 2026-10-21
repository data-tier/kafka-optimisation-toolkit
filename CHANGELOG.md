# Changelog

## v0.1.0 (2026-10-21)

First release.

- A small Apache Kafka 4.3.0 cluster that runs in Docker: three brokers and three controllers, no ZooKeeper. Each node has its own config file that sets only what it needs to start, so everything else is Kafka's default.
- Metrics collection from every broker and controller into Prometheus, limited to the metrics the book uses.
- `make cluster-up`, `cluster-down`, `cluster-reset` and `cluster-health` to start, stop, wipe and check the cluster.
- `bench/sweep.sh`, which tests one producer, consumer or broker setting across a list of values. Broker settings are changed live where Kafka allows it, otherwise the brokers are restarted.
- Recording of broker metrics during every run, a full description of the machine for every set of results, and a report with a table and chart for each sweep.
- The seven sweeps the book uses: linger, batch size, compression (on realistic JSON and on random data), acks, fetch minimum, replica fetchers, and poll size.
- Two small Java add-ons: one sends random data that cannot be compressed, the other is a consumer that spends 1 ms on each record, like a real application.
- `migration-lab/`: a scripted practice run of moving a cluster from ZooKeeper to KRaft, following the Kafka 3.9 documentation, including both ways of rolling back and a data check after every step.
- SOURCES.md, which records where every default, option and metric used was checked, down to the line of Kafka's source code.
