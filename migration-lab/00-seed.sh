#!/usr/bin/env bash
# Step 00: start the ZooKeeper-mode cluster and give it metadata worth migrating.
#
# Starts 3 ZooKeeper nodes and 3 Kafka 3.9.1 brokers in ZooKeeper mode with
# inter.broker.protocol.version=3.9 (the page's prerequisite), then creates:
#   orders  6 partitions, RF 3, 1000 keyed records (50 distinct keys)
#   events  3 partitions, RF 3, 1000 records without keys
#   lab-orders  a consumer group that has read 600 records of orders and committed
# and records a baseline that verify.sh compares against after every step.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
require_state 00-seed none

say "Step 00: start the ZooKeeper ensemble and three brokers in ZooKeeper mode"
for id in "${BROKER_IDS[@]}"; do write_broker_config "$id" zk; done
note "broker-1 configuration:"
sed 's/^/      /' "$(broker_conf 1)"
run compose up -d zk1 zk2 zk3 broker-1 broker-2 broker-3 client

say "Wait for all three brokers to register in ZooKeeper"
waited=0
until [[ "$(zk ls /brokers/ids | grep -o '^\[.*\]$' | tail -1)" == "[1, 2, 3]" ]]; do
  (( waited < 180 )) || die "brokers 1, 2 and 3 did not all register in /brokers/ids within 180 seconds."
  sleep 3; waited=$((waited + 3))
done
printf '+ zookeeper-shell.sh zk1:2181 ls /brokers/ids\n'
note "$(zk ls /brokers/ids | grep -o '^\[.*\]$' | tail -1)"
printf '+ zookeeper-shell.sh zk1:2181 get /controller\n'
note "$(zk_get /controller)"

say "Create the topics"
run kc /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --topic orders --partitions 6 --replication-factor 3
run kc /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --topic events --partitions 3 --replication-factor 3
wait_urp_zero

say "Produce 1000 keyed records to orders and 1000 unkeyed records to events"
printf '+ kafka-console-producer.sh --topic orders --property parse.key=true --property key.separator=: < (1000 lines key-N:order-M)\n'
for i in $(seq 0 999); do printf 'key-%d:order-%d\n' $((i % 50)) "$i"; done \
  | kc /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic orders \
      --producer-property acks=all --property parse.key=true --property key.separator=:
printf '+ kafka-console-producer.sh --topic events < (1000 lines event-N)\n'
for i in $(seq 0 999); do printf 'event-%d\n' "$i"; done \
  | kc /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic events --producer-property acks=all

say "Consume 600 records of orders as group lab-orders and commit"
run kc /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" --topic orders \
  --group lab-orders --from-beginning --max-messages 600 > /dev/null

say "Record the baseline that verify.sh checks after every step"
mkdir -p "$RUN/baseline"
"$LAB/verify.sh" --record-baseline

evidence "topics and consumer group"
run kc /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic orders,events
run kc /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" --describe --group lab-orders

set_state 00-seed
