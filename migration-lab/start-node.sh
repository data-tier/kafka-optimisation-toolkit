#!/usr/bin/env bash
# Starts one Kafka 3.9.1 node from run/<node>/server.properties.
#
# The image's own launcher assumes a KRaft node and merges in its bundled
# defaults, so the lab bypasses it and starts Kafka on its own file. Brokers
# get JMX on port 9999 so the lab can read UnderReplicatedPartitions.
# Controllers get the lab's log4j.properties with the TRACE logger the
# migration guide recommends.
set -euo pipefail
: "${LAB_ROLE:?LAB_ROLE must be broker or controller}"
conf=/mnt/lab/conf/server.properties
[[ -r "$conf" ]] || { echo "start-node: $conf is missing" >&2; exit 1; }
echo "start-node: starting with $conf"
sed 's/^/start-node:   /' "$conf"
if [[ "$LAB_ROLE" == broker ]]; then
  export JMX_PORT=9999
  host=$(hostname)
  export KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=$host"
else
  export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:/mnt/lab/conf/log4j.properties"
fi
exec /opt/kafka/bin/kafka-server-start.sh "$conf"
