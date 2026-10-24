# Shared helpers for the bench and cluster scripts. Source it, do not run it.
# shellcheck shell=bash
# This file is sourced by the other scripts, which use the variables it
# defines, so ShellCheck's "appears unused" warning does not apply here.
# shellcheck disable=SC2034

KOT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
KOT_COMPOSE_FILE="$KOT_ROOT/cluster/docker-compose.yml"
KOT_BOOTSTRAP="broker-1:9092,broker-2:9092,broker-3:9092"
KOT_PROMETHEUS_URL="http://127.0.0.1:${KOT_PROMETHEUS_PORT:-9090}"
KOT_BROKERS=(broker-1 broker-2 broker-3)
# Must match scrape_interval in cluster/prometheus/prometheus.yml.
KOT_SCRAPE_INTERVAL=5
KOT_BROKER_IDS=(1 2 3)

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }

die() {
  printf '\nERROR: %s\n' "$*" >&2
  exit 1
}

compose() { docker compose -f "$KOT_COMPOSE_FILE" "$@"; }

# Runs a command in the client container. Stdin is not attached.
kclient() { compose exec -T client "$@"; }

# Runs a Kafka CLI tool from /opt/kafka/bin in the client container.
ktool() {
  local tool=$1; shift
  kclient "/opt/kafka/bin/$tool" "$@"
}

require_docker() {
  command -v docker >/dev/null 2>&1 || die "docker is not installed or not on PATH."
  docker info >/dev/null 2>&1 || die "the Docker daemon is not reachable. Start Docker and retry."
  docker compose version >/dev/null 2>&1 || die "the docker compose plugin is missing."
}

# Fails unless the client container is up. Every tool runs from it.
require_cluster_running() {
  require_docker
  local state
  state=$(compose ps --format '{{.Service}} {{.State}}' 2>/dev/null || true)
  grep -q '^client running$' <<<"$state" \
    || die "the cluster is not running (client container is not up). Run 'make cluster-up' first."
}

# Prints "voters observers" as reported by kafka-metadata-quorum.sh describe --status.
quorum_counts() {
  local status voters observers
  status=$(ktool kafka-metadata-quorum.sh --bootstrap-server "$KOT_BOOTSTRAP" describe --status 2>/dev/null) || return 1
  voters=$(grep '^CurrentVoters:' <<<"$status" | grep -o '"id": *[0-9]*' | wc -l | tr -d ' ')
  observers=$(grep '^CurrentObservers:' <<<"$status" | grep -o '"id": *[0-9]*' | wc -l | tr -d ' ')
  echo "$voters $observers"
}

# Number of exporter targets Prometheus currently reports as up (0 if unreachable).
prometheus_targets_up() {
  curl -fsS --max-time 5 "$KOT_PROMETHEUS_URL/api/v1/query" --data-urlencode 'query=sum(up{job="kafka"})' 2>/dev/null \
    | python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(int(float(r[0]["value"][1])) if r else 0)' 2>/dev/null \
    || echo 0
}

# Waits until 3 voters, 3 brokers (observers) and all 6 exporter targets are up.
wait_for_cluster() {
  local timeout=${1:-180} waited=0 counts up
  while true; do
    counts=$(quorum_counts || echo "0 0")
    up=$(prometheus_targets_up)
    if [[ "$counts" == "3 3" && "$up" == 6 ]]; then
      log "cluster ready: 3 controllers voting, 3 brokers registered, 6/6 exporter targets up"
      return 0
    fi
    if (( waited >= timeout )); then
      die "cluster not ready after ${timeout}s (voters/observers: ${counts}, exporter targets up: ${up}/6). Check 'docker compose -f cluster/docker-compose.yml logs'."
    fi
    sleep 3; waited=$((waited + 3))
  done
}

# Warns when the Docker engine has less memory or CPU than the cluster wants.
# Six Kafka JVMs start with Kafka's default -Xms1G -Xmx1G heap each.
warn_low_resources() {
  local mem cpus
  mem=$(docker info --format '{{.MemTotal}}')
  cpus=$(docker info --format '{{.NCPU}}')
  if (( mem < 12 * 1024 * 1024 * 1024 )); then
    log "WARNING: Docker has $(awk -v m="$mem" 'BEGIN {printf "%.1f", m / 1024 / 1024 / 1024}') GB of memory. The six Kafka nodes take 1 GB each and Kafka needs memory to spare for file caching, so 12 GB or more is recommended. With less, memory pressure will affect the results."
  fi
  if (( cpus < 4 )); then
    log "WARNING: Docker has only $cpus CPUs. The brokers, controllers and test tools all share them."
  fi
}

# Docker Desktop and Colima only share some host directories with their VM
# (by default the home directory). A bind mount from anywhere else silently
# becomes an empty directory, so check that the repository is visible first.
require_bind_mounts() {
  local image=$1
  docker run --rm --entrypoint test -v "$KOT_ROOT/cluster/prometheus:/kot-check:ro" "$image" -f /kot-check/prometheus.yml \
    || die "Docker cannot see files in $KOT_ROOT. Docker Desktop and Colima only share some host directories with their VM (by default your home directory). Move the checkout under a shared directory or add $KOT_ROOT to the VM's file sharing settings."
}
