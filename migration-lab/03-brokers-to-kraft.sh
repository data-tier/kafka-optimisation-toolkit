#!/usr/bin/env bash
# Step 03: migrate the brokers to KRaft ("Migrating brokers to KRaft" in the
# 3.9 guide).
#
# Each broker is rewritten as the guide's sample KRaft broker: process.roles=broker
# is added, broker.id becomes node.id (same id), and zookeeper.connect,
# zookeeper.metadata.migration.enable and inter.broker.protocol.version are
# removed. The quorum settings stay. Brokers are restarted one at a time,
# waiting for UnderReplicatedPartitions to return to 0 between each.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
require_state 03-brokers-to-kraft 02-brokers-migration-mode

for id in "${BROKER_IDS[@]}"; do
  say "Step 03: restart broker-$id as a KRaft broker"
  old=$(cat "$(broker_conf "$id")")
  write_broker_config "$id" kraft
  show_change "$(broker_conf "$id")" "$old"
  roll_broker "$id"
  note "brokers listed under CurrentObservers: $(observers)"
done

evidence "kafka-metadata-quorum.sh describe --status"
printf '+ kafka-metadata-quorum.sh --bootstrap-controller %s describe --status\n' "$QUORUM_BOOTSTRAP"
quorum_status
obs=$(observers)
for id in "${BROKER_IDS[@]}"; do
  [[ " $obs" == *" $id "* ]] || die "broker $id is not listed under CurrentObservers ($obs)."
done
note "brokers 1, 2 and 3 are all listed under CurrentObservers"

set_state 03-brokers-to-kraft
