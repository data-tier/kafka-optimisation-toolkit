#!/usr/bin/env bash
# Writes environment.json describing the machine, Docker, the Kafka image and
# JVM, the disks under the Kafka log directories and each node's effective
# server.properties.
#
# Usage: bench/environment.sh <output-file>
#
# Readers need every field to judge the numbers, so this script fails instead
# of writing a blank. If a disk model cannot be read (for example a cloud VM
# whose virtual disk exposes no model), set KOT_DISK_MODEL and KOT_DISK_TYPE to
# describe it and the JSON records that they were supplied by hand.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

out=${1:?usage: bench/environment.sh <output-file>}
require_cluster_running

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# Writes the value of a field to $tmp/<name>, failing if it is empty.
put() {
  local name=$1 value=$2
  [[ -n "$value" ]] || die "environment.sh: could not determine '$name'."
  printf '%s' "$value" > "$tmp/$name"
}

# ---------------------------------------------------------------- host
host_os=$(uname -s)
put host_kernel "$(uname -sr)"
case "$host_os" in
  Darwin)
    put host_os_release "$(sw_vers -productName) $(sw_vers -productVersion) ($(sw_vers -buildVersion))"
    put host_cpu_model "$(sysctl -n machdep.cpu.brand_string)"
    put host_cpu_cores_logical "$(sysctl -n hw.logicalcpu)"
    put host_cpu_cores_physical "$(sysctl -n hw.physicalcpu)"
    put host_ram_bytes "$(sysctl -n hw.memsize)"
    ;;
  Linux)
    put host_os_release "$(. /etc/os-release && echo "$PRETTY_NAME")"
    model=$(awk -F': *' '/^model name/{print $2; exit}' /proc/cpuinfo)
    if [[ -z "$model" ]] && command -v lscpu >/dev/null; then
      model=$(lscpu | awk -F': *' '/^Model name/{print $2; exit}')
    fi
    put host_cpu_model "$model"
    put host_cpu_cores_logical "$(nproc --all)"
    put host_cpu_cores_physical "$(lscpu -p=Core,Socket 2>/dev/null | grep -v '^#' | sort -u | wc -l | tr -d ' ')"
    put host_ram_bytes "$(awk '/^MemTotal:/{print $2 * 1024}' /proc/meminfo)"
    ;;
  *) die "environment.sh: unsupported host OS '$host_os' (Linux and macOS are supported)." ;;
esac

# ---------------------------------------------------------------- docker
put docker_client_version "$(docker version --format '{{.Client.Version}}')"
put docker_server_version "$(docker version --format '{{.Server.Version}}')"
put docker_context "$(docker context show)"
put docker_engine_os "$(docker info --format '{{.OperatingSystem}}')"
put docker_engine_kernel "$(docker info --format '{{.KernelVersion}}')"
put docker_engine_cpus "$(docker info --format '{{.NCPU}}')"
put docker_engine_mem_bytes "$(docker info --format '{{.MemTotal}}')"

# ---------------------------------------------------------------- kafka image and JVM
image=$(docker inspect "$(compose ps -q broker-1)" --format '{{.Config.Image}}')
put toolkit_image "$image"
put toolkit_image_id "$(docker image inspect "$image" --format '{{.Id}}')"
put kafka_image "apache/kafka:${KOT_KAFKA_TAG:-4.3.0}"
put kafka_image_digest "$(docker image inspect "apache/kafka:${KOT_KAFKA_TAG:-4.3.0}" --format '{{index .RepoDigests 0}}')"
put kafka_version "$(compose exec -T broker-1 sh -c 'ls /opt/kafka/libs' | sed -n 's/^kafka-clients-\(.*\)\.jar$/\1/p')"
put jvm_version "$(compose exec -T broker-1 java -version 2>&1)"

# ---------------------------------------------------------------- per node
for node in broker-1 broker-2 broker-3 controller-1 controller-2 controller-3; do
  # PID 1 is the Kafka JVM because cluster/start-node.sh execs it.
  put "jvm_cmdline_$node" "$(compose exec -T "$node" sh -c 'tr "\0" "\n" < /proc/1/cmdline')"
  put "server_properties_$node" "$(compose exec -T "$node" cat /tmp/kot-server.properties)"
done

for node in broker-1 broker-2 broker-3; do
  # mountinfo gives the real major:minor even when the source shows as /dev/root.
  info=$(compose exec -T "$node" sh -c '
    set -e
    line=$(grep " /var/lib/kafka/data " /proc/self/mountinfo)
    majmin=$(echo "$line" | awk "{print \$3}")
    fstype=$(echo "$line" | sed "s/.* - //" | awk "{print \$1}")
    opts=$(grep " /var/lib/kafka/data " /proc/mounts | awk "{print \$4}")
    dev=$(basename "$(readlink -f /sys/dev/block/$majmin)")
    blk=$dev
    if [ -f "/sys/class/block/$dev/partition" ]; then blk=$(basename "$(dirname "$(readlink -f /sys/class/block/$dev)")"); fi
    model=$(cat "/sys/block/$blk/device/model" 2>/dev/null | sed "s/ *$//" || true)
    vendor=$(cat "/sys/block/$blk/device/vendor" 2>/dev/null | sed "s/ *$//" || true)
    rot=$(cat "/sys/block/$blk/queue/rotational")
    printf "%s\n%s\n%s\n%s\n%s\n%s\n%s\n" "$dev" "$blk" "$fstype" "$opts" "$model" "$vendor" "$rot"
  ') || die "environment.sh: could not inspect the log dir mount in $node."
  { read -r dev; read -r blk; read -r fstype; read -r opts; read -r model; read -r vendor; read -r rot; } <<<"$info"
  put "logdir_device_$node" "$dev"
  put "logdir_block_device_$node" "$blk"
  put "logdir_fstype_$node" "$fstype"
  put "logdir_mount_options_$node" "$opts"
  put "logdir_rotational_$node" "$rot"
  printf '%s' "$model" > "$tmp/logdir_model_$node"
  printf '%s' "$vendor" > "$tmp/logdir_vendor_$node"
done

# ---------------------------------------------------------------- disk model and type
model=$(cat "$tmp/logdir_model_broker-1")
vendor=$(cat "$tmp/logdir_vendor_broker-1")
blk=$(cat "$tmp/logdir_block_device_broker-1")
rot=$(cat "$tmp/logdir_rotational_broker-1")
if [[ -n "${KOT_DISK_MODEL:-}" || -n "${KOT_DISK_TYPE:-}" ]]; then
  put disk_model "${KOT_DISK_MODEL:-}"
  put disk_type "${KOT_DISK_TYPE:-}"
  put disk_source "Supplied by hand through KOT_DISK_MODEL and KOT_DISK_TYPE."
elif [[ -n "$model" ]]; then
  put disk_model "$model"
  if [[ "$rot" == 0 ]]; then put disk_type "non-rotational (SSD or NVMe)"; else put disk_type "rotational (HDD)"; fi
  put disk_source "Read from /sys/block/$blk inside broker-1 (model and queue/rotational)."
elif [[ "$host_os" == Darwin ]]; then
  # The Docker engine runs in a Linux VM. Its virtio disk (vendor 0x1af4 has no
  # model, and virtio reports rotational=1 regardless of the media) is an image
  # file on the host. Find the directory that holds the VM for this context.
  ctx=$(cat "$tmp/docker_context")
  case "$ctx" in
    colima*) vmdir="${COLIMA_HOME:-$HOME/.colima}/_lima" ;;
    desktop-linux|default) vmdir="$HOME/Library/Containers/com.docker.docker" ;;
    orbstack) vmdir="$HOME/.orbstack" ;;
    *) vmdir="" ;;
  esac
  [[ -n "$vmdir" && -d "$vmdir" ]] \
    || die "environment.sh: cannot locate the VM disk image for Docker context '$ctx'. Set KOT_DISK_MODEL and KOT_DISK_TYPE to describe the host disk and rerun."
  vol=$(df "$vmdir" | awk 'NR==2{print $1}')
  store=$(diskutil info "$vol" | awk -F': *' '/APFS Physical Store/{print $2}')
  [[ -n "$store" ]] || die "environment.sh: could not find the APFS physical store behind $vol."
  phys=$(sed -E 's/s[0-9]+$//' <<<"$store")
  media=$(diskutil info "$phys" | awk -F': *' '/Device \/ Media Name/{print $2}')
  ssd=$(diskutil info "$phys" | awk -F': *' '/Solid State/{print $2}')
  proto=$(diskutil info "$phys" | awk -F': *' '/Protocol/{print $2; exit}')
  put disk_model "$media"
  case "$ssd" in
    Yes) put disk_type "SSD ($proto), seen by Kafka as a virtio-blk virtual disk" ;;
    No) put disk_type "rotational ($proto), seen by Kafka as a virtio-blk virtual disk" ;;
    *) die "environment.sh: diskutil did not report Solid State for $phys." ;;
  esac
  put disk_source "Kafka log dirs are on $blk (virtio-blk, vendor $vendor) inside the Docker VM (context $ctx). The VM directory $vmdir is on host volume $vol, APFS physical store $store, physical disk $phys, read with diskutil."
else
  die "environment.sh: $blk inside the containers exposes no model (vendor '${vendor:-none}'), so the disk cannot be identified automatically. Set KOT_DISK_MODEL and KOT_DISK_TYPE to describe the disk under the Docker data root and rerun."
fi

put generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
put toolkit_commit "$(git -C "$KOT_ROOT" rev-parse HEAD)$(git -C "$KOT_ROOT" diff --quiet HEAD -- . ':!results' 2>/dev/null || echo '-dirty')"

python3 - "$tmp" "$out" <<'EOF'
import json, os, sys
d, out = sys.argv[1], sys.argv[2]
v = lambda k: open(os.path.join(d, k)).read()
brokers = ["broker-1", "broker-2", "broker-3"]
nodes = brokers + ["controller-1", "controller-2", "controller-3"]

def jvm_flags(cmdline):
    # The JVM options are the arguments between the java binary and the main
    # class, minus the classpath (-cp and its value).
    args = cmdline.split("\n")[1:]
    flags, i = [], 0
    while i < len(args) and args[i].startswith("-"):
        if args[i] in ("-cp", "-classpath"):
            i += 2
            continue
        flags.append(args[i])
        i += 1
    return flags

env = {
    "generated_at": v("generated_at"),
    "toolkit_commit": v("toolkit_commit"),
    "host": {
        "os_release": v("host_os_release"),
        "kernel": v("host_kernel"),
        "cpu_model": v("host_cpu_model"),
        "cpu_cores_logical": int(v("host_cpu_cores_logical")),
        "cpu_cores_physical": int(v("host_cpu_cores_physical")),
        "ram_total_bytes": int(v("host_ram_bytes")),
    },
    "docker": {
        "client_version": v("docker_client_version"),
        "server_version": v("docker_server_version"),
        "context": v("docker_context"),
        "engine_os_release": v("docker_engine_os"),
        "engine_kernel": v("docker_engine_kernel"),
        "engine_cpus": int(v("docker_engine_cpus")),
        "engine_ram_total_bytes": int(v("docker_engine_mem_bytes")),
    },
    "kafka": {
        "image": v("kafka_image"),
        "image_digest": v("kafka_image_digest"),
        "toolkit_image": v("toolkit_image"),
        "toolkit_image_id": v("toolkit_image_id"),
        "version": v("kafka_version"),
    },
    "jvm": {
        "version": v("jvm_version"),
        "flags": {n: jvm_flags(v("jvm_cmdline_" + n)) for n in nodes},
    },
    "disk": {
        "model": v("disk_model"),
        "type": v("disk_type"),
        "how_determined": v("disk_source"),
    },
    "kafka_log_dirs": {
        b: {
            "path": "/var/lib/kafka/data",
            "device": v("logdir_device_" + b),
            "block_device": v("logdir_block_device_" + b),
            "filesystem": v("logdir_fstype_" + b),
            "mount_options": v("logdir_mount_options_" + b),
            "rotational_flag": v("logdir_rotational_" + b),
        } for b in brokers
    },
    "server_properties": {n: v("server_properties_" + n) for n in nodes},
}

def check(x, path="environment"):
    if isinstance(x, dict):
        for k, val in x.items():
            check(val, path + "." + k)
    elif isinstance(x, list):
        if not x:
            sys.exit("environment.sh: %s is empty" % path)
    elif x in ("", None):
        sys.exit("environment.sh: %s is empty" % path)
check(env)
for n in nodes:
    if not any(f.startswith("-Xmx") for f in env["jvm"]["flags"][n]):
        sys.exit("environment.sh: no -Xmx found in the JVM flags of %s" % n)
with open(out, "w") as f:
    json.dump(env, f, indent=2)
    f.write("\n")
EOF
log "wrote $out"
