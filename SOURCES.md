# Sources

This file backs up every factual claim the toolkit makes. Nothing in it was written from memory: each setting, command-line option, metric name and tool behaviour the toolkit relies on was checked against Kafka's own source code or official documentation, and the table says where. It is a reference to look things up in, not something to read from start to finish. All checks were made on **2026-10-21**.

Line numbers refer to Kafka's source code on GitHub. Kafka source references are to the `4.3` branch at commit `05bf7f5f38786dc9251661fdae3a5dc4fb698d09` (`gradle.properties` says `4.3.3-SNAPSHOT`), fetched from `https://github.com/apache/kafka` and `https://raw.githubusercontent.com/apache/kafka/4.3/`. The perf tools, `ToolsUtils`, `MetadataQuorumCommand`, `ConfigCommand`, `RequestMetrics`, `SharedServer` and `ProducerConfig` are byte-identical between that commit and tag `4.3.0` (commit `a9ce3221537b8653448750697915607dc7936cf3`), which is the release in the Docker image. Line numbers refer to that commit.

## Images and artifacts

| Item | Value | Checked against |
|---|---|---|
| Kafka image | `apache/kafka:4.3.0`, digest `sha256:0be4c9eb3565733612d2836d65636fd611a219ebcf5b4162e5ac259ea0ecb907` | Docker Hub tag list (`hub.docker.com/v2/repositories/apache/kafka/tags`). `4.3.0` exists. `4.3.1` and `4.3.2` also exist. The toolkit pins 4.3.0 as requested |
| Image contents | Temurin JRE 21.0.11+10 on Alpine 3.23, no `javac`, Kafka in `/opt/kafka`, `kafka-clients-4.3.0.jar` | `docker/jvm/Dockerfile` and inspection of the pulled image |
| Image launcher | `/etc/kafka/docker/run` merges the bundled `docker/server.properties` (which sets e.g. `offsets.topic.replication.factor=1`, `num.partitions=1`) into the final config | `docker/jvm/launch`, `docker/server.properties` |
| JDK build image | `eclipse-temurin:21.0.11_10-jdk-alpine` (same Java build as the runtime) | Docker Hub tag `21.0.11_10-jdk-alpine` |
| Prometheus | `prom/prometheus:v3.15.0` | Docker Hub tag `v3.15.0` |
| JMX exporter | `jmx_prometheus_javaagent-1.7.0.jar`, SHA-256 `0ad843261c567d1924a017d542e14c40f2afff523d940c25ee000742fde4b3a9` | GitHub release `prometheus/jmx_exporter` 1.7.0 and its published `.sha256` file. The Dockerfile verifies the checksum with `ADD --checksum` |
| Exporter config keys | `includeObjectNames` (allowlist), `excludeJvmMetrics`, `rules` with `pattern`, `name`, `type`, `labels`. With rules present, unmatched attributes are not exported | `collector/src/main/java/io/prometheus/jmx/JmxCollector.java` at tag 1.7.0, lines 446-560 |
| Exporter self-metrics | The agent also serves its own `jmx_scrape_*`, `jmx_config_reload_*`, `jmx_exporter_build_info` and `jmx_http_requests_rejected_total` series. These are not MBeans | Observed on the running exporter |

## Broker configuration (KRaft)

From `https://kafka.apache.org/43/configuration/broker-configs/` (Type, Default and Update Mode rows).

| Key | 4.3 default | Update mode | Used how |
|---|---|---|---|
| `process.roles` | none | read-only | set to `broker` or `controller` |
| `node.id` | none | read-only | 1-3 brokers, 101-103 controllers |
| `controller.quorum.voters` | `""` | read-only | static voter set. The docs call this "the old way" and prefer `controller.quorum.bootstrap.servers` for dynamic quorums. A fixed benchmark quorum does not need dynamic membership |
| `controller.listener.names` | none | read-only | `CONTROLLER` |
| `listeners` | `PLAINTEXT://:9092` | per-broker | brokers `PLAINTEXT://:9092`, controllers `CONTROLLER://:9093` |
| `advertised.listeners` | `null` (falls back to `listeners`) | read-only | `PLAINTEXT://broker-N:9092` |
| `listener.security.protocol.map` | `SASL_SSL:SASL_SSL,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT` (no `CONTROLLER`) | per-broker | adds `CONTROLLER:PLAINTEXT` |
| `log.dirs` | `null` (falls back to `log.dir`, default `/tmp/kafka-logs`) | read-only | `/var/lib/kafka/data` |
| `num.replica.fetchers` | `1` | cluster-wide | swept by `make sweep-fetchers` through `kafka-configs.sh --alter --entity-default` |
| `min.insync.replicas` | `1` | cluster-wide | left at default. Bench topics set it to 2 as a topic config |
| `offsets.topic.replication.factor` | `3` | read-only | default kept (3 brokers) |
| `transaction.state.log.replication.factor` / `.min.isr` | `3` / `2` | read-only | default kept |
| `auto.create.topics.enable` | `true` | read-only | default kept. sweep.sh always creates topics explicitly |
| `replica.fetch.wait.max.ms` | `500` | read-only | default kept. Explains a FetchFollower RemoteTimeMs near 500 ms when followers are idle |
| `replica.fetch.max.bytes` | `1048576` | read-only | used once in testing the restart path |
| `message.max.bytes` | `1048588` | cluster-wide | default kept |
| `group.initial.rebalance.delay.ms` | `3000` | read-only | default kept. It adds about 3 s of join time to every new consumer group |
| `num.network.threads` / `num.io.threads` | `3` / `8` | cluster-wide | default kept |

`bench/broker-update-modes.tsv` holds the update mode of all 343 broker configs on that page (65 cluster-wide, 37 per-broker, 241 read-only), generated by `bench/tools/broker_update_modes.py`. `sweep.sh` uses it to choose a dynamic alter (cluster-wide or per-broker) or a restart with the key in `server.properties` (read-only).

Storage formatting: `kafka-storage.sh format --cluster-id <id> --config <file>`. Options in `core/src/main/scala/kafka/tools/StorageTool.scala` lines 318 (`--cluster-id`) and 396 (`--config`).

## Producer configuration

From `https://kafka.apache.org/43/configuration/producer-configs/`.

| Key | 4.3 default | Note |
|---|---|---|
| `linger.ms` | `5` | changed from 0 to 5 in Kafka 4.0 (stated on the page) |
| `batch.size` | `16384` | |
| `compression.type` | `none` | sweep values `none`, `lz4`, `snappy`, `zstd`, `gzip` |
| `acks` | `all` | sweep.sh passes `acks=all` unless acks is under test |
| `enable.idempotence` | `true` | `acks=1` disables idempotence silently when `enable.idempotence` was not set by the user: `ProducerConfig.java` lines 591-613 |
| `max.request.size` | `1048576` | |
| `buffer.memory` | `33554432` | |
| `metrics.sample.window.ms` / `metrics.num.samples` | `30000` / `2` | so client `-avg` and `-max` metrics cover roughly the last 30 to 60 seconds |

## Consumer configuration

From `https://kafka.apache.org/43/configuration/consumer-configs/`.

| Key | 4.3 default | Note |
|---|---|---|
| `fetch.min.bytes` | `1` | swept by `make sweep-fetch-min` |
| `fetch.max.wait.ms` | `500` | |
| `max.poll.records` | `500` | swept by `make sweep-poll` |
| `max.partition.fetch.bytes` | `1048576` | overwritten by kafka-consumer-perf-test, see below |
| `group.protocol` | `classic` | |
| `auto.offset.reset` | `latest` | overwritten to `earliest` by kafka-consumer-perf-test. SlowConsumer sets `latest` explicitly |
| `metrics.sample.window.ms` / `metrics.num.samples` | `30000` / `2` | |

## kafka-producer-perf-test (`tools/src/main/java/org/apache/kafka/tools/ProducerPerformance.java`)

| Behaviour | Lines |
|---|---|
| `--command-property` takes one or more `k=v` after a single flag (`nargs("+")`) and is the current option | 324-331 |
| `--producer-props` is deprecated in favour of `--command-property`, and the two cannot be combined | 314-322, 614-616 |
| `--num-records` (required), `--throughput` (required, `-1` disables throttling), `--record-size` | 262-268, 307-312, 270-277 |
| `--record-size`, `--payload-file` and `--payload-monotonic` are mutually exclusive, one is required | 249-253 |
| `--record-size` payloads are random uppercase letters (`random.nextInt(26) + 65`), regenerated in one reused buffer per record, from `SplittableRandom(0)` | 72-77, 93, 170-183 |
| `--payload-file` works only for UTF-8 text, is split on `--payload-delimiter` (default newline), and a line is picked at random per record | 279-287, 297-305, 173, 206-228 |
| Properties are split on `=` and must have exactly two pieces. `client.id` defaults to `perf-producer-client` | 185-204 |
| Without `--print-metrics` the producer is closed before the summary is printed. With it the producer is flushed, the summary printed, then metrics printed. sweep.sh always passes `--print-metrics` so every run takes the same path | 124-149 |
| Final summary format: `%d records sent, %f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.` with MB = 1024 x 1024 bytes | 505-521 |
| Percentiles come from at most 500,000 sampled latencies | 425-426, 523-532 |
| A failed send prints a stack trace and is not counted, so a short count means failures | 552-565 |
| `kafka-producer-perf-test.sh` sets `KAFKA_HEAP_OPTS=-Xmx512M` when unset. The bundled random-bytes producer uses the same | `bin/kafka-producer-perf-test.sh` |

## kafka-consumer-perf-test (`tools/src/main/java/org/apache/kafka/tools/ConsumerPerformance.java`)

| Behaviour | Lines |
|---|---|
| `--num-records` is the current option. `--messages` is deprecated, and exactly one of them is required | 338-346, 371, 374-376 |
| `--command-property` (repeatable `k=v`) passes consumer properties | 309-313 |
| `props()` overwrites `bootstrap.servers`, `group.id`, `receive.buffer.bytes` (from `--socket-buffer-size`, default 2 MiB), `max.partition.fetch.bytes` (from `--fetch-size`, default 1 MiB), `auto.offset.reset` (`earliest`), the deserializers and `check.crcs=false` after reading `--command-property`. sweep.sh refuses to sweep those keys | 400-421 |
| `--timeout` (max ms between returned records) defaults to 10000. sweep.sh passes 60000 | 333-337 |
| `--group` defaults to a random `perf-consumer-N`. sweep.sh passes a unique group per run | 299-303 |
| Header and result line: `start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec` | 92-103, 117-123 |
| A timeout prints `WARNING: Exiting before consuming the expected number of records`, which parse_perf.py treats as a failure | 177-179 |
| `kafka-consumer-perf-test.sh` sets `KAFKA_HEAP_OPTS=-Xmx512M` when unset | `bin/kafka-consumer-perf-test.sh` |

## --print-metrics format (`tools/src/main/java/org/apache/kafka/tools/ToolsUtils.java`)

Each line is `group:name:{tags} : value`, doubles printed with `%.3f` (lines 37-60).

## Client metric names

| Metric | Group | Source |
|---|---|---|
| `compression-rate-avg` | `producer-metrics` | `clients/.../producer/internals/SenderMetricsRegistry.java` line 85, an `Avg` (Sender.java 998-999), defined as compressed size over uncompressed size |
| `batch-size-avg` | `producer-metrics` | SenderMetricsRegistry.java 81 |
| `records-per-request-avg` | `producer-metrics` | SenderMetricsRegistry.java 100 |
| `request-latency-avg` | `producer-metrics` | SenderMetricsRegistry.java 92 |
| `record-queue-time-avg` | `producer-metrics` | SenderMetricsRegistry.java 88 |
| `record-retry-total`, `record-error-total` | `producer-metrics` | SenderMetricsRegistry.java 104, 108 |
| `bufferpool-wait-ratio` | `producer-metrics` | `producer/internals/BufferPool.java` 80. Group constant `PRODUCER_METRIC_GROUP_NAME` in KafkaProducer.java 246 |
| `fetch-latency-avg`, `fetch-size-avg`, `records-per-request-avg`, `fetch-rate`, `records-lag-max` | `consumer-fetch-manager-metrics` | `consumer/internals/FetchMetricsRegistry.java` 73 (group name), 75, 85, 92, 96, 101. `records-lag-max` is "based on current offset and not committed offset", meaning the fetch position |
| `poll-idle-ratio-avg` | `consumer-metrics` | `consumer/internals/metrics/KafkaConsumerMetrics.java` 44, 66-69. Group constant in `ConsumerUtils.java` 67-71 |

## Admin CLI tools

| Tool and options | Source |
|---|---|
| `kafka-metadata-quorum.sh --bootstrap-server ... describe --status` and its output fields (ClusterId, LeaderId, LeaderEpoch, HighWatermark, MaxFollowerLag, MaxFollowerLagTimeMs, CurrentVoters, CurrentObservers) | `tools/.../MetadataQuorumCommand.java` lines 104, 178-183, `handleDescribeStatus` |
| `kafka-topics.sh --create --partitions --replication-factor --config --delete --describe --list --under-replicated-partitions` | `tools/.../TopicCommand.java` lines 730-786 |
| `kafka-configs.sh --alter --describe --all --entity-type brokers --entity-default --entity-name --add-config --delete-config` | `core/src/main/scala/kafka/admin/ConfigCommand.scala` lines 559-586 |
| `kafka-configs.sh --describe --all` line format `  key=value sensitive=... synonyms={...}` | Observed output of the 4.3.0 tool |

## JVM launch

| Behaviour | Source |
|---|---|
| Default heap `-Xmx1G -Xms1G` for the server | `bin/kafka-server-start.sh` lines 28-29 |
| Default `KAFKA_JVM_PERFORMANCE_OPTS` (`-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent -XX:MaxInlineLevel=15`) | `bin/kafka-run-class.sh` lines 282-286 |
| `JMX_PORT` sets `com.sun.management.jmxremote.port` and `.rmi.port`. `KAFKA_JMX_OPTS` replaces the default JMX options | `bin/kafka-run-class.sh` lines 201-211 |
| `CLASSPATH` from the environment is kept, which is how the bundled classes are added | `bin/kafka-run-class.sh` |

## MBeans

Every name below appears on `https://kafka.apache.org/43/operations/monitoring/` and was matched against the source that registers it.

| MBean | Type and attribute exported | Source |
|---|---|---|
| `kafka.network:type=RequestMetrics,name={TotalTimeMs,RequestQueueTimeMs,LocalTimeMs,RemoteTimeMs,ResponseQueueTimeMs,ResponseSendTimeMs},request=*` | Histogram: `50thPercentile`, `99thPercentile`, `Max` | `server/src/main/java/org/apache/kafka/network/metrics/RequestMetrics.java` 50-54, 91-105. Request names `FetchConsumer` and `FetchFollower` at 37-38 |
| `kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent` | Meter: `OneMinuteRate` (idle fraction, marked in ns with a ns rate unit) | `core/.../KafkaRequestHandler.scala` 120, 249 |
| `kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent` | Gauge: `Value` | `core/.../network/SocketServer.scala` 116 |
| `kafka.network:type=RequestChannel,name=RequestQueueSize` | Gauge: `Value` | `core/.../network/RequestChannel.scala` 46 |
| `kafka.server:type=ReplicaFetcherManager,name=MaxLag,clientId=Replica` | Gauge: `Value` | `core/.../AbstractFetcherManager.scala` 46-54, `ReplicaFetcherManager.scala` 36 |
| `kafka.server:type=BrokerTopicMetrics,name={BytesInPerSec,BytesOutPerSec}` (all-topic form, no `topic` tag) | Meter: `Count`, turned into a rate by PromQL `rate(...[30s])` | monitoring page ("Omitting topic=(...) will yield the all-topic rate") |
| `kafka.server:type=ReplicaManager,name={UnderReplicatedPartitions,UnderMinIsrPartitionCount}` | Gauge: `Value` | `core/.../ReplicaManager.scala` 93-94 |
| `kafka.server:type=ReplicaManager,name=IsrShrinksPerSec` | Meter: `Count`, rate by PromQL | ReplicaManager.scala 100 |
| `kafka.controller:type=KafkaController,name=ActiveControllerCount` | Gauge: `Value`. **Controller processes only** | `metadata/.../controller/metrics/QuorumControllerMetrics.java` 47, created in `core/.../ControllerServer.scala` 233. Listed under "KRaft Controller Monitoring Metrics" |
| `kafka.controller:type=KafkaController,name=OfflinePartitionsCount` | Gauge: `Value`. **Controller processes only** | `metadata/.../controller/metrics/ControllerMetadataMetrics.java` 62, created only when `process.roles` contains `controller` in `core/.../SharedServer.scala` 279-284 |

Because the two controller MBeans never exist on a dedicated broker, the exporter also runs on the three controllers with an allowlist of just those two (`cluster/jmx-exporter/controller.yml`).

### Histogram semantics

`RequestMetrics` registers its histograms with `biased = true` (`newHistogram(name, true, tags)`, RequestMetrics.java 93-105, `KafkaMetricsGroup.java` 109). In Yammer metrics-core 2.2.0 (the `metrics-core-2.2.0.jar` in `/opt/kafka/libs`), a biased histogram uses `ExponentiallyDecayingSample(1028, 0.015)` and `max()` reads an `AtomicLong` that holds the largest value since the histogram was created (`com/yammer/metrics/core/Histogram.java` lines 20-21, 43-46, 55, 130 in `metrics-core-2.2.0-sources.jar` from Maven Central). So broker percentiles weight recent requests over a window of a minute or two, and `Max` is a lifetime maximum since broker start.

## Producer send path (why the random-bytes producer is safe)

`KafkaProducer.send(ProducerRecord, Callback)` is public and overridable (`clients/.../producer/KafkaProducer.java` 241, 959). The value is serialized and appended to the batch inside `send()` (lines 1007, 1029), and `DefaultRecord.writeTo` copies the value bytes into the batch (`clients/src/main/java/org/apache/kafka/common/record/internal/DefaultRecord.java` 168-196). The upstream perf tool relies on this when it reuses one payload buffer. `RandomBytesProducerPerformance` overwrites that buffer with random bytes just before calling `super.send`. `ProducerPerformance.start` and `createKafkaProducer` are package-private (lines 61, 162), which is why the class lives in `org.apache.kafka.tools`. The `kafka-tools` jar is not sealed, so this loads.
