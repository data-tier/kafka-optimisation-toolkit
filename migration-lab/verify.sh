#!/usr/bin/env bash
# Checks that the data and offsets seeded by 00-seed.sh are intact.
#
#   ./verify.sh                    compare against the baseline, print PASS or FAIL
#   ./verify.sh --record-baseline  write the baseline (called by 00-seed.sh)
#
# For each topic it reads every record from the beginning with its partition,
# offset and key, and compares the count and a SHA-256 of the sorted lines with
# the baseline. For lab-orders it compares the committed offset of every
# partition. It runs against the brokers in whatever mode they are in.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

snapshot_topic() {
  kc /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" --topic "$1" \
    --from-beginning --max-messages 1000 --timeout-ms 30000 \
    --consumer-property enable.auto.commit=false \
    --property print.partition=true --property print.offset=true --property print.key=true 2>/dev/null \
    | sort
}

snapshot_group() {
  kc /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" --describe --group lab-orders 2>/dev/null \
    | awk '$1 == "lab-orders" {print $2, $3, $4}' | sort
}

summarise() {
  local name=$1 data=$2
  printf '%s records=%s sha256=%s\n' "$name" "$(grep -c . <<<"$data" || true)" "$(printf '%s' "$data" | shasum -a 256 | cut -c1-16)"
}

current() {
  summarise orders "$(snapshot_topic orders)"
  summarise events "$(snapshot_topic events)"
  echo "lab-orders committed offsets:"
  snapshot_group | sed 's/^/  /'
}

if [[ "${1:-}" == "--record-baseline" ]]; then
  current > "$RUN/baseline/snapshot.txt"
  grep -q '^orders records=1000 ' "$RUN/baseline/snapshot.txt" || die "baseline: orders does not hold 1000 records."
  grep -q '^events records=1000 ' "$RUN/baseline/snapshot.txt" || die "baseline: events does not hold 1000 records."
  [[ $(grep -c '^  orders ' "$RUN/baseline/snapshot.txt") == 6 ]] || die "baseline: lab-orders does not have offsets for 6 partitions."
  sed 's/^/    /' "$RUN/baseline/snapshot.txt"
  exit 0
fi

[[ -f "$RUN/baseline/snapshot.txt" ]] || die "no baseline. Run ./00-seed.sh first."
say "verify.sh (state: $(current_state))"
now=$(current)
fail=0
check() {
  local label=$1 want=$2 got=$3
  if [[ "$want" == "$got" ]]; then
    printf '    PASS  %s\n' "$label"
  else
    printf '    FAIL  %s\n          expected: %s\n          got:      %s\n' "$label" "$want" "$got"
    fail=1
  fi
}
base=$(cat "$RUN/baseline/snapshot.txt")
check "orders: 1000 records, same partitions, offsets, keys and values" "$(grep '^orders ' <<<"$base")" "$(grep '^orders ' <<<"$now")"
check "events: 1000 records, same partitions, offsets and values" "$(grep '^events ' <<<"$base")" "$(grep '^events ' <<<"$now")"
check "lab-orders: committed offsets on all 6 partitions" "$(grep '^  ' <<<"$base" | tr '\n' ' ')" "$(grep '^  ' <<<"$now" | tr '\n' ' ')"
if (( fail )); then
  printf '    RESULT: FAIL\n'
  exit 1
fi
printf '    RESULT: PASS\n'
