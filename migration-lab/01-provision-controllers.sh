#!/usr/bin/env bash
# Step 01: provision the KRaft controller quorum ("Provisioning the KRaft
# controller quorum" in the 3.9 guide).
#
# The guide's migration section formats one controller with --standalone. Its
# "Controller membership changes" section, which the migration section points
# to, recommends bootstrapping one voter with --standalone and adding the rest
# with kafka-metadata-quorum.sh add-controller after formatting them with
# --no-initial-controllers. This script does exactly that:
#   controller-3000  format --standalone, start
#   controller-3001  format --no-initial-controllers, start, add-controller
#   controller-3002  format --no-initial-controllers, start, add-controller
set -euo pipefail
source "$(dirname "$0")/lib.sh"
require_state 01-provision-controllers 00-seed rolled-back-from-step-2 rolled-back-from-step-3

say "Step 01: read the cluster ID from ZooKeeper"
printf '+ zookeeper-shell.sh zk1:2181 get /cluster/id\n'
cluster_json=$(zk_get /cluster/id)
note "$cluster_json"
cluster_id=$(grep -o '"id":"[^"]*"' <<<"$cluster_json" | cut -d'"' -f4)
[[ -n "$cluster_id" ]] || die "could not read the cluster id from /cluster/id."
note "cluster id: $cluster_id"

say "Write the controller configuration (migration enabled) and the TRACE logger for the migration components"
for id in "${CONTROLLER_IDS[@]}"; do
  write_controller_config "$id" migration
  write_controller_log4j "$id"
done
note "controller-3000 configuration:"
sed 's/^/      /' "$(controller_conf 3000)"

format() {
  local id=$1; shift
  run composec run --rm --no-deps --entrypoint /opt/kafka/bin/kafka-storage.sh "controller-$id" \
    format "$@" --cluster-id="$cluster_id" -c /mnt/lab/conf/server.properties
}

say "Format controller-3000 as a standalone voter and start it"
format 3000 --standalone
run composec up -d controller-3000
wait_quorum 1
note "controller-3000 is the leader of a one-voter quorum"

for id in 3001 3002; do
  say "Format controller-$id with --no-initial-controllers, start it, and add it to the quorum"
  format "$id" --no-initial-controllers
  run composec up -d "controller-$id"
  note "waiting for controller-$id to appear as an observer"
  waited=0
  until quorum_status 2>/dev/null | grep '^CurrentObservers:' | grep -q "\"id\": $id"; do
    (( waited < 120 )) || die "controller-$id did not appear in CurrentObservers within 120 seconds."
    sleep 3; waited=$((waited + 3))
  done
  run kc /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-controller "$QUORUM_BOOTSTRAP" describe --replication
  # add-controller reads the new controller's own config and meta.properties,
  # so it runs inside that controller's container.
  run composec exec -T "controller-$id" /opt/kafka/bin/kafka-metadata-quorum.sh \
    --command-config /mnt/lab/conf/server.properties --bootstrap-controller "$QUORUM_BOOTSTRAP" add-controller
  # 3001 makes two voters, 3002 makes three.
  wait_quorum $(( id - 2999 ))
done

evidence "kafka-metadata-quorum.sh describe --status"
printf '+ kafka-metadata-quorum.sh --bootstrap-controller %s describe --status\n' "$QUORUM_BOOTSTRAP"
quorum_status
voters=$(quorum_status | grep '^CurrentVoters:' | grep -o '"id": *[0-9]*' | grep -o '[0-9]*$' | sort -n | tr '\n' ' ')
[[ "$voters" == "3000 3001 3002 " ]] || die "expected voters 3000 3001 3002, got: $voters"
note "voters: $voters"

set_state 01-provision-controllers
