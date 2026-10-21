# Migration lab sources

Where every setting, command and behaviour in the migration lab comes from. The lab is meant to follow the Kafka 3.9 documentation exactly, so this file shows, for each step, which part of that documentation (or of Kafka's source code) it was checked against. All checks were made on **2026-10-21**.

- **The procedure**: "ZooKeeper to KRaft Migration" on `https://kafka.apache.org/39/operations/kraft/#zookeeper-to-kraft-migration`, page footer "Last modified June 16, 2026: MINOR: Fix typo (#882) (e9923300f1)". Section names below refer to that page.
- **3.9 config defaults**: `https://kafka.apache.org/39/configuration/broker-configs/`.
- **Kafka source**: tag `3.9.1` of `https://github.com/apache/kafka` (commit `f745dfdcee2b9851204ddbbcd423626ab87294bc`). Line numbers refer to that commit.

## Where the brief and the page differed

The lab follows the page. These were raised and agreed before the lab was built.

| Brief | Page | Lab |
|---|---|---|
| `apache/kafka:3.9.0` | "Preparing for migration": "the Kafka brokers must be upgraded to software version 3.9.1" | `apache/kafka:3.9.1` |
| `rmr /controller`, `rmr /migration` | revert table: "run `delete /controller`", "run `get /migration` followed by `delete /migration`" | `delete`, as the page says. `rmr` is not in the ZooKeeper 3.5+ shell anyway |
| no epoch step | both revert rows start (step 3 row: after the first roll) with comparing `get /controller_epoch` against `kraft_controller_epoch` in `get /migration` and `set /controller_epoch <value>` if the KRaft epoch is higher (KAFKA-20488) | included |
| "one with `--standalone` ... or all three with `--initial-controllers`" | the migration section shows only `format --standalone`. "Controller membership changes" recommends one `--standalone` voter, then `add-controller` for the rest after formatting them with `--no-initial-controllers` | 3000 `--standalone`, 3001 and 3002 `--no-initial-controllers` + `add-controller` |

One more difference, between the page and the software: the page quotes the completion log line as `Completed migration of metadata from Zookeeper to KRaft`. Kafka 3.9.1 logs `Completed migration of metadata from ZooKeeper to KRaft.` with a capital K (`metadata/src/main/java/org/apache/kafka/metadata/migration/KRaftMigrationDriver.java` line 789). `02-brokers-migration-mode.sh` matches it case-insensitively and prints the real line.

## Images

| Image | Verified |
|---|---|
| `apache/kafka:3.9.1`, digest `sha256:4ceccc577f03f51f6af8dbfda55194d0d892f4fa7913ffbded567ce3895622ed` | Docker Hub tag `3.9.1`. `3.9.0` and `3.9.2` also exist. Contains `kafka_2.13-3.9.1.jar`, `zookeeper-3.8.4.jar`, Temurin 21.0.7, `bin/zookeeper-shell.sh` |
| `zookeeper:3.8.4`, digest `sha256:20d3c93ebda093d0c90911493396975e0e04523af225d8053d42bab781f9ca48` | Docker Hub official image tag `3.8.4`. Matches the ZooKeeper client Kafka 3.9.1 is built with (`gradle/dependencies.gradle` line 162) |
| ZooKeeper ensemble settings `ZOO_MY_ID`, `ZOO_SERVERS` (`server.N=host:2888:3888;2181`), volumes `/data` and `/datalog` | Docker Hub official `zookeeper` image description, "Environment variables" and its replicated-mode compose example |

## Configuration keys

### ZooKeeper-mode brokers (`00-seed.sh`)

| Key | Value | Verified against |
|---|---|---|
| `broker.id` | 1, 2, 3 | broker configs page (default -1). "Enter Migration Mode": set it explicitly |
| `listeners` | `PLAINTEXT://:9092` | page sample broker config |
| `advertised.listeners` | `PLAINTEXT://broker-N:9092` | page sample (with the Compose host name in place of `localhost`) |
| `log.dirs` | `/var/lib/kafka/data` | broker configs page (default null) |
| `inter.broker.protocol.version` | `3.9` | "Preparing for migration" and the sample broker config |
| `zookeeper.connect` | `zk1:2181,zk2:2181,zk3:2181` | page sample (`zookeeper.connect=localhost:2181`) |

### Brokers in migration mode (`02-brokers-migration-mode.sh`)

The page's list: `broker.id`, `controller.quorum.bootstrap.servers`, `controller.listener.names`, the controller listener added to `listener.security.protocol.map`, and `zookeeper.metadata.migration.enable`. Values follow the page's sample broker config:

| Key | Value |
|---|---|
| `listener.security.protocol.map` | `PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT` |
| `zookeeper.metadata.migration.enable` | `true` (default false) |
| `controller.quorum.bootstrap.servers` | `controller-3000:9093,controller-3001:9093,controller-3002:9093`. "Static versus Dynamic KRaft Quorums": it "should contain as many as possible" |
| `controller.listener.names` | `CONTROLLER` |

### KRaft brokers (`03-brokers-to-kraft.sh`)

"Migrating brokers to KRaft" and its sample: add `process.roles=broker`, replace `broker.id` with `node.id` (same id), remove `inter.broker.protocol.version`, `zookeeper.metadata.migration.enable` and `zookeeper.connect`, keep `controller.quorum.bootstrap.servers` and `controller.listener.names`. The lab sets no `control.plane.listener.name` or `authorizer.class.name`, so the page's notes about those do not apply.

### Controllers (`01-provision-controllers.sh`)

The page's sample controller config, key for key:

| Key | Value |
|---|---|
| `process.roles` | `controller` |
| `node.id` | 3000, 3001, 3002. The page: KRaft node ids must differ from every ZooKeeper `broker.id` |
| `controller.quorum.bootstrap.servers` | all three controllers |
| `controller.listener.names` | `CONTROLLER` |
| `listeners` | `CONTROLLER://:9093` |
| `zookeeper.metadata.migration.enable` | `true` |
| `zookeeper.connect` | `zk1:2181,zk2:2181,zk3:2181` |
| `inter.broker.listener.name` | `PLAINTEXT` ("The inter broker listener in brokers to allow KRaft controller send RPCs to brokers") |

Two keys the sample leaves under `# Other configs ...`:

| Key | Value | Why |
|---|---|---|
| `advertised.listeners` | `CONTROLLER://controller-N:9093` | `format --standalone` and `add-controller` take the voter endpoint from the controller's listener config, and an empty host becomes `localhost` (`tools/src/main/java/org/apache/kafka/tools/MetadataQuorumCommand.java` lines 390-417). With each controller in its own container, `localhost` would point every voter at itself |
| `log.dirs` | `/var/lib/kafka/data` | the Docker volume for the metadata log |

`listener.security.protocol.map` is not set on controllers, as in the sample. In KRaft mode, when the map is not set explicitly, Kafka 3.9.1 maps the controller listener names to PLAINTEXT itself (`core/src/main/scala/kafka/server/KafkaConfig.scala` lines 838-852).

### Finalized controllers (`04-finalize.sh`)

"Finalizing the migration" and its sample: remove `zookeeper.metadata.migration.enable` and `zookeeper.connect`, restart the controllers one at a time.

### Rollbacks

| Script | Page row | Edits, in the page's order |
|---|---|---|
| `rollback-from-step-2.sh` | "Enter Migration Mode on the brokers" | epoch check, deprovision the quorum, `delete /controller`, `get /migration`, `delete /migration`, then on each broker remove `zookeeper.metadata.migration.enable`, `controller.listener.names` and `controller.quorum.bootstrap.servers` and replace `node.id` with `broker.id` (a no-op, migration-mode brokers already use `broker.id`), one rolling restart |
| `rollback-from-step-3.sh` | "Migrating brokers to KRaft" | on each broker remove `process.roles`, replace `node.id` with `broker.id`, restore `zookeeper.connect`, with `zookeeper.metadata.migration.enable=true` on this first roll ("Make sure that on the first cluster roll, zookeeper.metadata.migration.enable remains set to true"), first rolling restart, then the epoch check, deprovision, `delete /controller`, `get /migration`, `delete /migration`, then remove `zookeeper.metadata.migration.enable`, `controller.listener.names` and `controller.quorum.bootstrap.servers`, second rolling restart |

The step 3 revert does not restore `inter.broker.protocol.version`, and the lab does not either. Its 3.9 default is `3.9-IV0` (broker configs page), which is the same version the cluster ran with, so the "do not change the metadata version during a migration" limitation is respected. The step 2 revert leaves `inter.broker.protocol.version=3.9` in place because migration-mode brokers still have it. The CONTROLLER entry in `listener.security.protocol.map` is not mentioned by either revert row and stays.

### Logging

"Preparing for migration" recommends `log4j.logger.org.apache.kafka.metadata.migration=TRACE` in each controller's `log4j.properties`. The lab copies the image's `/opt/kafka/config/log4j.properties`, appends that line and points the controllers at it with `KAFKA_LOG4J_OPTS=-Dlog4j.configuration=file:...`.

## Commands and flags

| Command | Verified against |
|---|---|
| `zookeeper-shell.sh zk1:2181 get /cluster/id` | "Provisioning the KRaft controller quorum" |
| `zookeeper-shell.sh ... get /controller_epoch`, `get /migration`, `set /controller_epoch <value>`, `delete /controller`, `delete /migration` | revert table |
| `kafka-storage.sh format --standalone --cluster-id=<id> -c <file>` | "Provisioning the KRaft controller quorum". Flags in `core/src/main/scala/kafka/tools/StorageTool.scala` lines 184 (`--config`/`-c`), 189 (`--cluster-id`/`-t`), 208 (`--standalone`) |
| `kafka-storage.sh format --no-initial-controllers ...` | "Controller membership changes" ("use the kafka-storage.sh format command with the --no-initial-controllers flag"), StorageTool.scala line 212 |
| `kafka-metadata-quorum.sh --command-config <controller config> --bootstrap-controller <controllers> add-controller` | "Controller membership changes". `MetadataQuorumCommand.java` lines 104-108 (connection options), 334 (`add-controller`). It reads `node.id`, the metadata log dir's `meta.properties` (directory id) and the listeners from the given file (lines 375-417), so the lab runs it inside the controller being added |
| `kafka-metadata-quorum.sh --bootstrap-controller ... describe --status` and `describe --replication` | MetadataQuorumCommand.java lines 176-187 |
| `kafka-server-start.sh <file>` | "Provisioning the KRaft controller quorum" |
| UnderReplicatedPartitions: `kafka-run-class.sh org.apache.kafka.tools.JmxTool --jmx-url service:jmx:rmi:///jndi/rmi://broker-N:9999/jmxrmi --object-name kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions --attributes Value --one-time true --report-format properties` | `tools/src/main/java/org/apache/kafka/tools/JmxTool.java` lines 312-359 (options) and 283 (properties output format). JMX on port 9999 through `JMX_PORT` in `bin/kafka-run-class.sh` |
| `kafka-console-producer.sh --property parse.key=true --property key.separator=:` | `core/src/main/scala/kafka/tools/ConsoleProducer.scala` lines 359-361 |
| `kafka-console-consumer.sh --property print.partition=true --property print.offset=true --property print.key=true` | `tools/src/main/java/org/apache/kafka/tools/consumer/DefaultMessageFormatter.java` lines 60-76 |
| `kafka-consumer-groups.sh --describe --group lab-orders` | `tools/src/main/java/org/apache/kafka/tools/consumer/group/ConsumerGroupCommandOptions.java` lines 135-189 |

## Evidence the steps look for

| Step | Evidence | Source |
|---|---|---|
| 02 | INFO line `Completed migration of metadata from ZooKeeper to KRaft` on the active controller | "Enter Migration Mode on the Brokers" note, KRaftMigrationDriver.java line 789 |
| 02 | `/controller` names the KRaft controller, `/migration` holds `kraft_controller_epoch` | revert table (the fields it tells you to read) |
| 03 | every broker under `CurrentObservers` in `describe --status` | brief. The page says only that brokers are restarted as KRaft brokers |
| rollbacks | `/controller` names a broker (1 to 3), `/migration` is gone, no migration or KRaft keys remain on brokers, no controller runs | revert table ("so that one of the brokers can become the new old-style controller", "clear the migration state") |
