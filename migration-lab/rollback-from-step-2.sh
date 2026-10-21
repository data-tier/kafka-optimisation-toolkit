#!/usr/bin/env bash
# Revert to ZooKeeper mode after step 02 ("Enter Migration Mode on the brokers"
# was the last completed step in the 3.9 guide's revert table).
#
# In the guide's order:
#   1. compare /controller_epoch with kraft_controller_epoch in /migration and
#      raise /controller_epoch if the KRaft epoch is higher (KAFKA-20488)
#   2. deprovision the KRaft controller quorum
#   3. delete /controller, then get /migration and delete /migration
#   4. on each broker remove zookeeper.metadata.migration.enable,
#      controller.listener.names and controller.quorum.bootstrap.servers, replace
#      node.id with broker.id, and do one rolling restart
set -euo pipefail
source "$(dirname "$0")/lib.sh"
require_state rollback-from-step-2 02-brokers-migration-mode

say "Rollback from step 02: revert to ZooKeeper mode (one broker roll)"
check_and_fix_controller_epoch
deprovision_controllers
clear_zk_controller_and_migration

to_zk_mode() {
  local f=$1
  conf_remove "$f" zookeeper.metadata.migration.enable controller.listener.names controller.quorum.bootstrap.servers
  # The guide also says to replace node.id with broker.id. A migration-mode
  # broker already uses broker.id, so this is a no-op here.
  conf_rename "$f" node.id broker.id
}
roll_all_brokers "rollback-from-step-2 roll" to_zk_mode

prove_zk_mode
"$LAB/verify.sh"
set_state rolled-back-from-step-2
