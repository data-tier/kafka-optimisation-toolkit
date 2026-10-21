#!/usr/bin/env bash
# Revert to ZooKeeper mode after step 03 ("Migrating brokers to KRaft" was the
# last completed step in the 3.9 guide's revert table).
#
# In the guide's order:
#   1. on each broker remove process.roles, replace node.id with broker.id and
#      restore zookeeper.connect. zookeeper.metadata.migration.enable must be
#      true on this first roll. Rolling restart of all brokers (first roll).
#   2. compare /controller_epoch with kraft_controller_epoch in /migration and
#      raise /controller_epoch if the KRaft epoch is higher (KAFKA-20488)
#   3. deprovision the KRaft controller quorum
#   4. delete /controller, then get /migration and delete /migration
#   5. on each broker remove zookeeper.metadata.migration.enable,
#      controller.listener.names and controller.quorum.bootstrap.servers, then
#      a second rolling restart
set -euo pipefail
source "$(dirname "$0")/lib.sh"
require_state rollback-from-step-3 03-brokers-to-kraft

say "Rollback from step 03: revert to ZooKeeper mode (two broker rolls)"

back_to_zk_broker() {
  local f=$1
  conf_remove "$f" process.roles
  conf_rename "$f" node.id broker.id
  conf_set "$f" zookeeper.connect "$ZK_CONNECT"
  # "Make sure that on the first cluster roll, zookeeper.metadata.migration.enable
  # remains set to true." Step 03 removed it, so it is set back to true here.
  conf_set "$f" zookeeper.metadata.migration.enable true
}
roll_all_brokers "rollback-from-step-3 first roll" back_to_zk_broker

check_and_fix_controller_epoch
deprovision_controllers
clear_zk_controller_and_migration

leave_migration() {
  local f=$1
  conf_remove "$f" zookeeper.metadata.migration.enable controller.listener.names controller.quorum.bootstrap.servers
}
roll_all_brokers "rollback-from-step-3 second roll" leave_migration

prove_zk_mode
"$LAB/verify.sh"
set_state rolled-back-from-step-3
