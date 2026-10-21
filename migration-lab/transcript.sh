#!/usr/bin/env bash
# Runs the whole lab from scratch and saves everything printed to
# transcript-<date>.log:
#
#   reset, 00, 01, 02, 03, rollback-from-step-3, 01, 02, 03, 04
#
# with verify.sh after every step. It stops at the first failure, and the log
# ends with the failing step. It takes about half an hour.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

log="$LAB/transcript-$(date +%Y-%m-%d).log"
[[ ! -e "$log" ]] || die "$log already exists. Move or delete it first."

exec > >(tee "$log") 2>&1

echo "ZooKeeper to KRaft migration lab transcript"
echo "started:          $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "toolkit commit:   $(git -C "$LAB" rev-parse --short HEAD)$(git -C "$LAB" diff --quiet HEAD -- . 2>/dev/null || echo ' (with local changes)')"
echo "docker:           $(docker version --format '{{.Server.Version}}') ($(docker info --format '{{.OperatingSystem}}'))"
echo "kafka image:      apache/kafka:3.9.1 $(docker image inspect apache/kafka:3.9.1 --format '{{index .RepoDigests 0}}' | cut -d@ -f2)"
echo "zookeeper image:  zookeeper:3.8.4 $(docker image inspect zookeeper:3.8.4 --format '{{index .RepoDigests 0}}' | cut -d@ -f2)"
echo "procedure:        https://kafka.apache.org/39/operations/kraft/#zookeeper-to-kraft-migration"

step() {
  printf '\n\n################################################################\n'
  printf '# %s   %s\n' "$(date -u +%H:%M:%S)" "$*"
  printf '################################################################\n'
  "$LAB/$1" "${@:2}"
  printf '\n# %s   verify.sh after %s\n' "$(date -u +%H:%M:%S)" "$1"
  "$LAB/verify.sh"
}

printf '\n# %s   reset.sh\n' "$(date -u +%H:%M:%S)"
"$LAB/reset.sh"

step 00-seed.sh
step 01-provision-controllers.sh
step 02-brokers-migration-mode.sh
step 03-brokers-to-kraft.sh
step rollback-from-step-3.sh
step 01-provision-controllers.sh
step 02-brokers-migration-mode.sh
step 03-brokers-to-kraft.sh
step 04-finalize.sh --yes

printf '\nfinished: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf 'transcript: %s\n' "${log#"$LAB"/}"
