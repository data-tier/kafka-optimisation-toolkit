#!/usr/bin/env bash
# Starts one Kafka node (broker or controller) from the checked-in
# cluster/config/<node>/server.properties.
#
# The official image's own launcher merges in a bundled server.properties that
# changes several defaults (for example offsets.topic.replication.factor=1).
# This script bypasses it so the node runs on Kafka 4.3 defaults plus only the
# lines in our file.
set -euo pipefail

: "${KOT_NODE:?KOT_NODE must name this node, for example broker-1}"
: "${KOT_ROLE:?KOT_ROLE must be broker or controller}"
: "${KOT_CLUSTER_ID:?KOT_CLUSTER_ID must be set to the cluster id}"

src=/opt/kot/config/server.properties
override=/opt/kot/overrides/broker.properties
# The effective file. bench/environment.sh reads it from here.
conf=/tmp/kot-server.properties

if [[ ! -r "$src" ]]; then
  echo "start-node: $src is not readable. Is cluster/config/$KOT_NODE mounted?" >&2
  exit 1
fi
cp "$src" "$conf"

# bench/sweep.sh writes cluster/overrides/broker.properties when it has to
# test a read-only broker setting. Later keys win in a Java properties file.
if [[ "$KOT_ROLE" == broker && -s "$override" ]]; then
  {
    echo
    echo "# Appended by cluster/start-node.sh from cluster/overrides/broker.properties"
    cat "$override"
  } >> "$conf"
  echo "start-node: applied broker overrides:"
  sed 's/^/start-node:   /' "$override"
fi

if [[ ! -f /var/lib/kafka/data/meta.properties ]]; then
  echo "start-node: formatting storage for $KOT_NODE"
  /opt/kafka/bin/kafka-storage.sh format --cluster-id "$KOT_CLUSTER_ID" --config "$conf"
fi

# JMX on a fixed port (9999) inside the container, plus the Prometheus JMX
# exporter agent on port 7071. These are exported here, not in the Compose
# environment, so tools run with docker exec do not try to bind the same ports.
export JMX_PORT=9999
export KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=$KOT_NODE"
export KAFKA_OPTS="-javaagent:/opt/kot/jmx_prometheus_javaagent.jar=7071:/opt/kot/jmx/$KOT_ROLE.yml"

exec /opt/kafka/bin/kafka-server-start.sh "$conf"
