#!/usr/bin/env bash
# Step 02: enter migration mode on the brokers ("Enter Migration Mode on the
# Brokers" in the 3.9 guide).
#
# Each broker gets the keys the guide lists: an explicit broker.id,
# controller.quorum.bootstrap.servers, controller.listener.names, the
# CONTROLLER listener in listener.security.protocol.map, and
# zookeeper.metadata.migration.enable=true. Brokers are restarted one at a time,
# waiting for UnderReplicatedPartitions to return to 0 between each. Once the
# last broker is back, the migration starts on its own, and the script waits
# for the active controller to log that it has completed.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
require_state 02-brokers-migration-mode 01-provision-controllers

for id in "${BROKER_IDS[@]}"; do
  say "Step 02: put broker-$id into migration mode and restart it"
  old=$(cat "$(broker_conf "$id")")
  write_broker_config "$id" migration
  show_change "$(broker_conf "$id")" "$old"
  roll_broker "$id"
done

say "Wait for the active controller to log that the migration completed"
leader=$(leader_id)
[[ "$leader" =~ ^300[0-2]$ ]] || die "could not find the active controller (LeaderId '$leader')."
note "active controller: controller-$leader"
printf '+ docker compose logs controller-%s | grep "Completed migration of metadata from ZooKeeper to KRaft"\n' "$leader"
# The 3.9 page writes "Zookeeper". Kafka 3.9.1 logs "ZooKeeper"
# (KRaftMigrationDriver.java line 789), so the match ignores case.
waited=0
until line=$(composec logs --no-log-prefix "controller-$leader" 2>/dev/null | grep -i 'Completed migration of metadata from Zookeeper to KRaft'); do
  (( waited < 300 )) || die "controller-$leader did not log 'Completed migration of metadata from ZooKeeper to KRaft' within 300 seconds."
  sleep 5; waited=$((waited + 5))
done

evidence "the active controller's log line"
printf '%s\n' "$line"
note "timestamp: $(grep -o '^\[[^]]*\]' <<<"$line" | head -1)"

evidence "ZooKeeper now records the KRaft controller and the migration state"
printf '+ zookeeper-shell.sh zk1:2181 get /controller\n'
note "$(zk_get /controller)"
printf '+ zookeeper-shell.sh zk1:2181 get /migration\n'
note "$(zk_get /migration)"

set_state 02-brokers-migration-mode
