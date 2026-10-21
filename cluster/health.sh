#!/usr/bin/env bash
# make cluster-health: prints the KRaft quorum status and the under-replicated
# partition count, and exits non-zero if the cluster is not healthy.
set -euo pipefail
source "$(dirname "$0")/../bench/lib.sh"

require_cluster_running

echo "== kafka-metadata-quorum.sh describe --status"
ktool kafka-metadata-quorum.sh --bootstrap-server "$KOT_BOOTSTRAP" describe --status \
  || die "kafka-metadata-quorum.sh describe --status failed."

echo
echo "== Under-replicated partitions (kafka-topics.sh --describe --under-replicated-partitions)"
urp_out=$(ktool kafka-topics.sh --bootstrap-server "$KOT_BOOTSTRAP" --describe --under-replicated-partitions) \
  || die "kafka-topics.sh --describe --under-replicated-partitions failed."
urp=$(grep -c 'Partition:' <<<"$urp_out" || true)
echo "under-replicated partitions: $urp"
[[ -n "$urp_out" ]] && echo "$urp_out"

echo
echo "== UnderReplicatedPartitions per broker (JMX via Prometheus)"
curl -fsS --max-time 5 "$KOT_PROMETHEUS_URL/api/v1/query" \
  --data-urlencode 'query=kot_gauge{mbean="UnderReplicatedPartitions"}' \
  | python3 -c '
import json, sys
res = json.load(sys.stdin)["data"]["result"]
if len(res) != 3:
    sys.exit("expected 3 brokers reporting UnderReplicatedPartitions, got %d" % len(res))
for r in sorted(res, key=lambda r: r["metric"]["node"]):
    print("%s: %s" % (r["metric"]["node"], r["value"][1]))
' || die "could not read UnderReplicatedPartitions from Prometheus at $KOT_PROMETHEUS_URL."

[[ "$urp" == 0 ]] || die "$urp partitions are under-replicated."
