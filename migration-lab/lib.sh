# Shared helpers for the migration lab scripts. Source it, do not run it.
# shellcheck shell=bash

LAB=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
RUN="$LAB/run"
STATE_FILE="$RUN/.state"
COMPOSE_FILE="$LAB/docker-compose.yml"

BROKER_IDS=(1 2 3)
CONTROLLER_IDS=(3000 3001 3002)
BOOTSTRAP="broker-1:9092,broker-2:9092,broker-3:9092"
ZK_CONNECT="zk1:2181,zk2:2181,zk3:2181"
QUORUM_BOOTSTRAP="controller-3000:9093,controller-3001:9093,controller-3002:9093"

# Any command that fails unexpectedly stops the script with its line and text,
# so a step can never die silently.
set -E
trap 'printf "\nERROR: %s line %s failed: %s\n" "${BASH_SOURCE[0]##*/}" "$LINENO" "$BASH_COMMAND" >&2' ERR

# ---------------------------------------------------------------- output

say() { printf '\n==> %s\n' "$*"; }
note() { printf '    %s\n' "$*"; }
evidence() { printf '\n--- evidence: %s\n' "$*"; }

die() {
  printf '\nERROR: %s\n' "$*" >&2
  exit 1
}

# Prints a command, then runs it. Tools run through kc are printed as the
# plain Kafka command, the way a reader would type it.
run() {
  if [[ "$1" == kc ]]; then
    printf '+ %s\n' "${2##*/} ${*:3}"
  elif [[ "$1" == compose || "$1" == composec ]]; then
    printf '+ docker compose %s\n' "${*:2}"
  else
    printf '+ %s\n' "$*"
  fi
  "$@"
}

compose() { docker compose -f "$COMPOSE_FILE" "$@"; }
# The controllers sit in the "controllers" profile so a plain `up` never starts them.
composec() { docker compose -f "$COMPOSE_FILE" --profile controllers "$@"; }

# Runs a Kafka tool in the client container (no JMX port, never a broker or controller).
kc() { compose exec -T client "$@"; }

# zookeeper-shell.sh exactly as the page uses it, run from the client container.
# zookeeper-shell.sh exits 1 for a missing znode, so callers inspect the output
# rather than the exit status.
zk() { kc /opt/kafka/bin/zookeeper-shell.sh zk1:2181 "$@" 2>&1 || true; }

# The data line of a znode (zookeeper-shell prints connection chatter around it).
zk_get() {
  zk get "$1" | grep -v -E '^(Connecting to|WATCHER::|WatchedEvent|$)' | tail -1
}

zk_exists() {
  local out
  out=$(zk get "$1")
  ! grep -q 'Node does not exist' <<<"$out"
}

quorum_status() {
  kc /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-controller "$QUORUM_BOOTSTRAP" describe --status
}

# ---------------------------------------------------------------- state

# The steps must run in order. .state holds the last completed step, or
# "<step>:in-progress" while a step is running. A failed step leaves the
# in-progress marker and blocks every other step until reset.sh is run.
current_state() { cat "$STATE_FILE" 2>/dev/null || echo "none"; }

require_state() {
  local step=$1; shift
  local state
  state=$(current_state)
  [[ "$state" != *:in-progress ]] \
    || die "$state: an earlier step did not finish. Inspect the cluster, then run ./reset.sh and start again."
  local ok
  for ok in "$@"; do
    if [[ "$state" == "$ok" ]]; then
      mkdir -p "$RUN"
      echo "$step:in-progress" > "$STATE_FILE"
      return 0
    fi
  done
  die "$step cannot run now. The last completed step is '$state', and $step needs one of: $*."
}

set_state() { echo "$1" > "$STATE_FILE"; say "state: $1"; }

# ---------------------------------------------------------------- config files

# Each node reads run/<node>/server.properties. The functions below write the
# exact configuration for each phase, so every file the lab uses can be read in
# one place. Keys and their sources are listed in SOURCES.md.

broker_conf() { echo "$RUN/broker-$1/server.properties"; }
controller_conf() { echo "$RUN/controller-$1/server.properties"; }

# phase: zk | migration | kraft
write_broker_config() {
  local id=$1 phase=$2 f
  f=$(broker_conf "$id")
  mkdir -p "$(dirname "$f")"
  {
    echo "# broker-$id, phase: $phase. Written by migration-lab/lib.sh."
    case "$phase" in
      zk|migration) echo "broker.id=$id" ;;
      kraft)
        echo "process.roles=broker"
        echo "node.id=$id"
        ;;
    esac
    echo "listeners=PLAINTEXT://:9092"
    echo "advertised.listeners=PLAINTEXT://broker-$id:9092"
    case "$phase" in
      migration|kraft) echo "listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT" ;;
    esac
    echo "log.dirs=/var/lib/kafka/data"
    case "$phase" in
      zk)
        echo "inter.broker.protocol.version=3.9"
        echo "zookeeper.connect=$ZK_CONNECT"
        ;;
      migration)
        echo "inter.broker.protocol.version=3.9"
        echo "zookeeper.metadata.migration.enable=true"
        echo "zookeeper.connect=$ZK_CONNECT"
        echo "controller.quorum.bootstrap.servers=$QUORUM_BOOTSTRAP"
        echo "controller.listener.names=CONTROLLER"
        ;;
      kraft)
        echo "controller.quorum.bootstrap.servers=$QUORUM_BOOTSTRAP"
        echo "controller.listener.names=CONTROLLER"
        ;;
      *) die "unknown broker phase $phase" ;;
    esac
  } > "$f.new"
  mv "$f.new" "$f"
}

# phase: migration | finalized
write_controller_config() {
  local id=$1 phase=$2 f
  f=$(controller_conf "$id")
  mkdir -p "$(dirname "$f")"
  {
    echo "# controller-$id, phase: $phase. Written by migration-lab/lib.sh."
    echo "process.roles=controller"
    echo "node.id=$id"
    echo "controller.quorum.bootstrap.servers=$QUORUM_BOOTSTRAP"
    echo "controller.listener.names=CONTROLLER"
    echo "listeners=CONTROLLER://:9093"
    echo "advertised.listeners=CONTROLLER://controller-$id:9093"
    echo "log.dirs=/var/lib/kafka/data"
    case "$phase" in
      migration)
        echo "zookeeper.metadata.migration.enable=true"
        echo "zookeeper.connect=$ZK_CONNECT"
        ;;
      finalized) ;;
      *) die "unknown controller phase $phase" ;;
    esac
    echo "inter.broker.listener.name=PLAINTEXT"
  } > "$f.new"
  mv "$f.new" "$f"
}

# Shows a config change as a diff, so the transcript records exactly what changed.
show_change() {
  local f=$1 old=$2
  printf '+ diff of %s\n' "${f#"$LAB"/}"
  diff -u <(printf '%s\n' "$old") "$f" | tail -n +3 || true
}

# The controllers' log4j.properties: the image's own file plus the TRACE logger
# the page recommends for the migration components.
write_controller_log4j() {
  local id=$1 f="$RUN/controller-$id/log4j.properties"
  mkdir -p "$(dirname "$f")"
  docker run --rm --entrypoint cat apache/kafka:3.9.1 /opt/kafka/config/log4j.properties > "$f"
  printf '\n# Recommended by the 3.9 migration guide while the migration is active.\nlog4j.logger.org.apache.kafka.metadata.migration=TRACE\n' >> "$f"
}

# ---------------------------------------------------------------- health

# UnderReplicatedPartitions from one broker's JMX, or empty if it cannot be read.
urp_of() {
  kc /opt/kafka/bin/kafka-run-class.sh org.apache.kafka.tools.JmxTool \
    --jmx-url "service:jmx:rmi:///jndi/rmi://broker-$1:9999/jmxrmi" \
    --object-name kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions \
    --attributes Value --one-time true --report-format properties 2>/dev/null \
    | sed -n 's/^kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions:Value=//p'
}

# Waits until every broker answers on JMX and all report UnderReplicatedPartitions=0
# on two reads 5 seconds apart.
wait_urp_zero() {
  local waited=0 good=0 id v line
  while true; do
    line="" ; local all=1
    for id in "${BROKER_IDS[@]}"; do
      # A broker that is still starting cannot be read yet, which counts as not ready.
      v=$(urp_of "$id" || true)
      line+=" broker-$id=${v:-unreachable}"
      [[ "$v" == 0 ]] || all=0
    done
    if (( all )); then
      good=$((good + 1))
      (( good >= 2 )) && { note "UnderReplicatedPartitions:$line"; return 0; }
    else
      good=0
    fi
    (( waited < 300 )) || die "UnderReplicatedPartitions did not return to 0 within 300 seconds:$line"
    sleep 5; waited=$((waited + 5))
  done
}

# Restarts one broker and waits for UnderReplicatedPartitions to return to 0.
roll_broker() {
  local id=$1
  run compose restart "broker-$id"
  note "waiting for UnderReplicatedPartitions to return to 0 on all brokers"
  wait_urp_zero
}

# Waits until the quorum answers with a leader and the given number of voters.
wait_quorum() {
  local voters=$1 waited=0 status n
  while true; do
    status=$(quorum_status 2>/dev/null || true)
    n=$(grep '^CurrentVoters:' <<<"$status" | grep -o '"id": *[0-9]*' | wc -l | tr -d ' ')
    if grep -q '^LeaderId: *[0-9]' <<<"$status" && [[ "$n" == "$voters" ]]; then
      return 0
    fi
    (( waited < 180 )) || die "the KRaft quorum did not report a leader and $voters voters within 180 seconds."
    sleep 3; waited=$((waited + 3))
  done
}

leader_id() { quorum_status | sed -n 's/^LeaderId: *//p'; }

# Every broker id listed under CurrentObservers.
observers() { quorum_status | grep '^CurrentObservers:' | grep -o '"id": *[0-9]*' | grep -o '[0-9]*$' | sort -n | tr '\n' ' '; }

# The node id of the ZooKeeper-mode controller from /controller, or empty.
zk_controller_brokerid() { zk_get /controller | grep -o '"brokerid":[0-9]*' | grep -o '[0-9]*$'; }

# ---------------------------------------------------------------- epochs (KAFKA-20488)

# Compares /controller_epoch with kraft_controller_epoch in /migration and raises
# /controller_epoch above it if needed, as both revert procedures on the page say.
check_and_fix_controller_epoch() {
  local zk_epoch migration kraft_epoch
  say "Compare the ZooKeeper controller epoch with the KRaft controller epoch"
  printf '+ zookeeper-shell.sh zk1:2181 get /controller_epoch\n'
  zk_epoch=$(zk_get /controller_epoch)
  note "/controller_epoch = $zk_epoch"
  printf '+ zookeeper-shell.sh zk1:2181 get /migration\n'
  migration=$(zk_get /migration)
  note "/migration = $migration"
  kraft_epoch=$(grep -o '"kraft_controller_epoch":[0-9-]*' <<<"$migration" | grep -o '[0-9-]*$')
  [[ "$zk_epoch" =~ ^[0-9]+$ ]] || die "could not read /controller_epoch (got '$zk_epoch')."
  [[ "$kraft_epoch" =~ ^-?[0-9]+$ ]] || die "could not read kraft_controller_epoch from /migration (got '$migration')."
  if (( kraft_epoch > zk_epoch )); then
    local value=$((kraft_epoch + 1))
    note "KRaft epoch $kraft_epoch is higher than ZooKeeper epoch $zk_epoch, so /controller_epoch is set to $value"
    printf '+ zookeeper-shell.sh zk1:2181 set /controller_epoch %s\n' "$value"
    zk set /controller_epoch "$value" >/dev/null
    note "/controller_epoch is now $(zk_get /controller_epoch)"
  else
    note "KRaft epoch $kraft_epoch is not higher than ZooKeeper epoch $zk_epoch, nothing to change"
  fi
}

# Stops and removes the controllers and their storage.
deprovision_controllers() {
  say "Deprovision the KRaft controller quorum"
  run composec rm -s -f -v controller-3000 controller-3001 controller-3002
  local id
  for id in "${CONTROLLER_IDS[@]}"; do
    run docker volume rm -f "kot-mig_controller-$id-data" >/dev/null
  done
}

# Deletes /controller, then reads and deletes /migration, as the page orders.
clear_zk_controller_and_migration() {
  say "Delete /controller so a broker can become the ZooKeeper-mode controller, then clear /migration"
  printf '+ zookeeper-shell.sh zk1:2181 delete /controller\n'
  zk delete /controller | grep -v -E '^(Connecting to|WATCHER::|WatchedEvent|$)' || true
  printf '+ zookeeper-shell.sh zk1:2181 get /migration\n'
  note "/migration = $(zk_get /migration)"
  printf '+ zookeeper-shell.sh zk1:2181 delete /migration\n'
  zk delete /migration | grep -v -E '^(Connecting to|WATCHER::|WatchedEvent|$)' || true
  zk_exists /migration && die "/migration still exists after delete."
  return 0
}

# Proves the cluster is back in pure ZooKeeper mode.
prove_zk_mode() {
  evidence "pure ZooKeeper mode"
  local waited=0 id
  while [[ -z "$(zk_controller_brokerid)" ]]; do
    (( waited < 120 )) || die "no ZooKeeper-mode controller registered in /controller within 120 seconds."
    sleep 3; waited=$((waited + 3))
  done
  id=$(zk_controller_brokerid)
  printf '+ zookeeper-shell.sh zk1:2181 get /controller\n'
  note "/controller = $(zk_get /controller)"
  [[ " ${BROKER_IDS[*]} " == *" $id "* ]] || die "/controller names $id, which is not one of the ZooKeeper-mode brokers."
  note "the ZooKeeper-mode controller is broker $id"
  if zk_exists /migration; then die "/migration still exists, so the cluster is not in pure ZooKeeper mode."; fi
  note "/migration does not exist"
  for id in "${BROKER_IDS[@]}"; do
    grep -qE '^(zookeeper.metadata.migration.enable|controller.quorum.bootstrap.servers|process.roles)' "$(broker_conf "$id")" \
      && die "broker-$id still has migration or KRaft settings."
  done
  note "no broker config contains zookeeper.metadata.migration.enable, controller.quorum.bootstrap.servers or process.roles"
  local running
  running=$(composec ps --services --status running | grep -c '^controller-' || true)
  [[ "$running" == 0 ]] || die "$running KRaft controllers are still running."
  note "no KRaft controller is running"
}

# ---------------------------------------------------------------- literal config edits (rollbacks)

# The revert procedures are written as edits ("remove X", "replace node.id with
# broker.id"), so the rollback scripts apply exactly those edits to the current file.
conf_remove() {
  local f=$1; shift
  local k
  for k in "$@"; do
    grep -v -E "^${k//./\\.}=" "$f" > "$f.new" || true
    mv "$f.new" "$f"
  done
}

conf_rename() {
  local f=$1 from=$2 to=$3
  sed "s/^${from//./\\.}=/${to}=/" "$f" > "$f.new"
  mv "$f.new" "$f"
}

conf_set() {
  local f=$1 k=$2 v=$3
  conf_remove "$f" "$k"
  echo "$k=$v" >> "$f"
}

# Rolls every broker, one at a time, after applying an edit function to its config.
# Usage: roll_all_brokers <label> <edit-function>
roll_all_brokers() {
  local label=$1 edit=$2 id old f
  for id in "${BROKER_IDS[@]}"; do
    say "$label: broker-$id"
    f=$(broker_conf "$id")
    old=$(cat "$f")
    "$edit" "$f" "$id"
    sed -i.bak "1s/.*/# broker-$id, phase: $label. Written by migration-lab\\/lib.sh./" "$f" && rm -f "$f.bak"
    show_change "$f" "$old"
    roll_broker "$id"
  done
}
