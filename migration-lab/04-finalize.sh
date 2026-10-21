#!/usr/bin/env bash
# Step 04: finalize the migration ("Finalizing the migration" in the 3.9 guide).
#
# Removes zookeeper.metadata.migration.enable and zookeeper.connect from each
# controller and restarts the controllers one at a time. After this the
# cluster can never go back to ZooKeeper mode.
#
#   ./04-finalize.sh         asks for confirmation
#   ./04-finalize.sh --yes   skips the question (used by transcript.sh)
set -euo pipefail
source "$(dirname "$0")/lib.sh"

cat <<'WARN'

  ##########################################################################
  #                                                                        #
  #   WARNING: FINALIZING THE MIGRATION CANNOT BE REVERTED.                #
  #                                                                        #
  #   "After the migration has been finalized, it is not possible to       #
  #    revert back to ZooKeeper mode." (Kafka 3.9 docs, Limitations)       #
  #                                                                        #
  #   After this step there is no rollback script. Run it only on the      #
  #   throwaway lab cluster.                                               #
  #                                                                        #
  ##########################################################################

WARN
if [[ "${1:-}" != "--yes" ]]; then
  read -r -p "Type FINALIZE to continue: " answer
  [[ "$answer" == FINALIZE ]] || die "not finalized."
fi
require_state 04-finalize 03-brokers-to-kraft

for id in "${CONTROLLER_IDS[@]}"; do
  say "Step 04: take controller-$id out of migration mode and restart it"
  old=$(cat "$(controller_conf "$id")")
  write_controller_config "$id" finalized
  show_change "$(controller_conf "$id")" "$old"
  # Restart time from the containers' clock, so it compares with the
  # timestamps the quorum reports.
  restart_ms=$(( $(kc date +%s) * 1000 ))
  run composec restart "controller-$id"
  wait_quorum 3
  note "waiting for controller-$id to lead, or to have caught up with the leader after its restart"
  waited=0
  until table=$(kc /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-controller "$QUORUM_BOOTSTRAP" describe --replication 2>/dev/null) \
        && awk -v id="$id" -v t="$restart_ms" '$1 == id && ($7 == "Leader" || ($4 == 0 && $6 > t)) {found=1} END {exit !found}' <<<"$table"; do
    (( waited < 120 )) || die "controller-$id did not rejoin the quorum within 120 seconds of its restart."
    sleep 3; waited=$((waited + 3))
  done
  printf '+ kafka-metadata-quorum.sh --bootstrap-controller %s describe --replication\n' "$QUORUM_BOOTSTRAP"
  printf '%s\n' "$table"
  note "controller-$id restarted at $restart_ms (ms) and is back in the quorum"
done

evidence "final kafka-metadata-quorum.sh describe --status"
printf '+ kafka-metadata-quorum.sh --bootstrap-controller %s describe --status\n' "$QUORUM_BOOTSTRAP"
quorum_status
for id in "${CONTROLLER_IDS[@]}"; do
  grep -qE '^zookeeper\.' "$(controller_conf "$id")" && die "controller-$id still has ZooKeeper settings."
done
note "no controller config contains zookeeper.metadata.migration.enable or zookeeper.connect"

set_state 04-finalize
