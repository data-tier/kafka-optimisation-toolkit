#!/usr/bin/env bash
# Tests one Kafka setting at several values and records the results.
#
# Usage:
#   bench/sweep.sh <side> <setting> <value1,value2,...> [options]
#
#   side      producer, consumer or broker: whose setting is being tested
#   setting   the setting to test, for example linger.ms
#   values    the values to try, separated by commas, for example 0,5,20
#
# Options:
#   --records N          records sent in each run (default 1000000)
#   --record-size B      size of each record in bytes (default 1024)
#   --runs R             runs per value, including the first, which is thrown
#                        away as a warm-up (default 3, at least 2)
#   --fixed k=v          another producer setting to hold at the same value for
#                        the whole sweep, for example --fixed linger.ms=20
#                        (can be given more than once)
#   --payload KIND       what each record contains: letters (default, random
#                        capital letters), json (realistic JSON events) or
#                        random-bytes (random data that cannot be compressed)
#   --label L            added to the name of the results folder
#   --consumer-app APP   perf (default) or slow. slow also runs a consumer that
#                        spends 1 ms on each record, for max.poll.records
#   --slow-duration S    how many seconds the slow consumer runs per value (default 60)
#   --slow-rate R        records per second sent while it runs (default 800)
#   -h, --help           show this help
#
# For each value it creates a new topic (12 partitions, each copied to all
# three brokers), applies the setting, runs Kafka's producer test the given
# number of times, runs Kafka's consumer test once, records the brokers'
# metrics throughout, and deletes the topic.
#
# Results go in results/<time>-<side>-<setting>[-<label>]/. The folder is
# marked INCOMPLETE until the whole sweep has succeeded, and the report will
# not use a folder that is still marked.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

# Prints the comment block at the top of this file. Exit status 0 for --help, 2 for a usage mistake.
usage() { awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "$0" >&2; exit "${1:-2}"; }

[[ "${1:-}" == -h || "${1:-}" == --help ]] && usage 0
[[ $# -ge 3 ]] || usage
command_line="bench/sweep.sh $*"
side=$1 setting=$2 values_csv=$3
shift 3

records=1000000
record_size=1024
runs=3
payload=letters
label=""
consumer_app=perf
slow_duration=60
slow_rate=800
fixed=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --records) records=${2:?}; shift 2 ;;
    --record-size) record_size=${2:?}; shift 2 ;;
    --runs) runs=${2:?}; shift 2 ;;
    --fixed) fixed+=("${2:?}"); shift 2 ;;
    --payload) payload=${2:?}; shift 2 ;;
    --label) label=${2:?}; shift 2 ;;
    --consumer-app) consumer_app=${2:?}; shift 2 ;;
    --slow-duration) slow_duration=${2:?}; shift 2 ;;
    --slow-rate) slow_rate=${2:?}; shift 2 ;;
    -h|--help) usage 0 ;;
    *) die "unknown option '$1'. Run bench/sweep.sh --help." ;;
  esac
done

# ---------------------------------------------------------------- validation
is_int() { [[ "$1" =~ ^[0-9]+$ ]]; }
case "$side" in producer|consumer|broker) ;; *) die "side must be producer, consumer or broker, got '$side'." ;; esac
[[ "$setting" =~ ^[a-z0-9][a-z0-9.-]*$ ]] || die "setting '$setting' does not look like a Kafka config key."
is_int "$records" && (( records > 0 )) || die "--records must be a positive integer."
is_int "$record_size" && (( record_size > 0 )) || die "--record-size must be a positive integer."
is_int "$runs" && (( runs >= 2 )) || die "--runs must be at least 2: the first run is discarded, so one run would leave nothing to keep."
is_int "$slow_duration" && (( slow_duration > 0 )) || die "--slow-duration must be a positive integer."
is_int "$slow_rate" && (( slow_rate > 0 )) || die "--slow-rate must be a positive integer."
case "$payload" in letters|json|random-bytes) ;; *) die "--payload must be letters, json or random-bytes." ;; esac
case "$consumer_app" in perf|slow) ;; *) die "--consumer-app must be perf or slow." ;; esac
[[ -z "$label" || "$label" =~ ^[A-Za-z0-9._-]+$ ]] || die "--label may only contain letters, digits, '.', '_' and '-'."
for kv in "${fixed[@]+"${fixed[@]}"}"; do
  [[ "$kv" =~ ^[a-z0-9.-]+=[^=[:space:]]+$ ]] || die "--fixed expects key=value, got '$kv'."
  [[ "${kv%%=*}" != "$setting" ]] || die "--fixed must not set the setting under test ($setting)."
done
if [[ "$consumer_app" == slow && "$side" != consumer ]]; then
  die "--consumer-app slow only makes sense for a consumer-side setting."
fi

IFS=',' read -r -a values <<<"$values_csv"
(( ${#values[@]} > 0 )) || die "no values given."
for v in "${values[@]}"; do
  # The value becomes part of a topic name, which allows [a-zA-Z0-9._-].
  [[ "$v" =~ ^[A-Za-z0-9._-]+$ ]] || die "value '$v' cannot be used in a topic name (allowed: letters, digits, '.', '_', '-')."
done

# Settings the perf tools set themselves, so the value under test would be ignored.
case "$side:$setting" in
  producer:bootstrap.servers|producer:key.serializer|producer:value.serializer|producer:transactional.id)
    die "$setting is set by kafka-producer-perf-test itself or by this script, so it cannot be swept." ;;
  consumer:bootstrap.servers|consumer:group.id|consumer:key.deserializer|consumer:value.deserializer|consumer:receive.buffer.bytes|consumer:max.partition.fetch.bytes|consumer:auto.offset.reset|consumer:check.crcs)
    die "kafka-consumer-perf-test overwrites $setting after reading --command-property (ConsumerPerformance.ConsumerPerfOptions.props() in Kafka 4.3, see SOURCES.md), so a sweep of it would silently test the tool's value." ;;
esac

broker_mode=""
if [[ "$side" == broker ]]; then
  broker_mode=$(awk -F'\t' -v k="$setting" '$1 == k {print $2}' "$KOT_ROOT/bench/broker-update-modes.tsv")
  [[ -n "$broker_mode" ]] || die "$setting is not in bench/broker-update-modes.tsv, so its update mode is unknown. Regenerate the table or check the spelling."
fi

require_cluster_running
wait_for_cluster 60
command -v python3 >/dev/null || die "python3 is required for the sampler and parsers."

# ---------------------------------------------------------------- output layout
stamp=$(date -u +%Y%m%dT%H%M%SZ)
out="$KOT_ROOT/results/$stamp-$side-$setting${label:+-$label}"
raw="$out/raw"
mkdir -p "$raw"
touch "$out/INCOMPLETE"
log "results directory: ${out#"$KOT_ROOT"/}"

echo "side,setting,value,run,kept,records_per_sec,mb_per_sec,avg_ms,max_ms,p50_ms,p95_ms,p99_ms,p999_ms" > "$out/runs.csv"
echo "side,setting,value,start_time,end_time,data_consumed_mb,mb_per_sec,records_consumed,records_per_sec,rebalance_time_ms,fetch_time_ms,fetch_mb_per_sec,fetch_records_per_sec" > "$out/consumer.csv"
echo "side,setting,value,run,client,metric,metric_value" > "$out/client-metrics.csv"
# start_ts and end_ts bracket the sampler: taken just before it starts and just after its
# final sample, so every broker sample of a run falls inside its window.
echo "side,setting,value,phase,run,start_ts,end_ts" > "$out/phases.csv"
if [[ "$consumer_app" == slow ]]; then
  echo "side,setting,value,records,elapsed_ms,records_per_sec,polls,poll_idle_ratio_avg,records_lag_max,app_backlog_records,producer_rate" > "$out/slow-consumer.csv"
fi

SWEEP_SIDE=$side SWEEP_SETTING=$setting SWEEP_VALUES=$values_csv SWEEP_RECORDS=$records \
SWEEP_RECORD_SIZE=$record_size SWEEP_RUNS=$runs SWEEP_PAYLOAD=$payload SWEEP_LABEL=$label \
SWEEP_CONSUMER_APP=$consumer_app SWEEP_SLOW_DURATION=$slow_duration SWEEP_SLOW_RATE=$slow_rate \
SWEEP_FIXED="${fixed[*]+"${fixed[*]}"}" SWEEP_BROKER_MODE=$broker_mode SWEEP_ARGV="$command_line" \
python3 - "$out/sweep.json" <<'EOF'
import json, os, sys
e = os.environ
json.dump({
    "command": e["SWEEP_ARGV"],
    "side": e["SWEEP_SIDE"], "setting": e["SWEEP_SETTING"],
    "values": e["SWEEP_VALUES"].split(","),
    "records": int(e["SWEEP_RECORDS"]), "record_size": int(e["SWEEP_RECORD_SIZE"]),
    "runs": int(e["SWEEP_RUNS"]), "discarded_runs": [1],
    "payload": e["SWEEP_PAYLOAD"], "label": e["SWEEP_LABEL"],
    "fixed_producer_properties": e["SWEEP_FIXED"].split() if e["SWEEP_FIXED"] else [],
    "consumer_app": e["SWEEP_CONSUMER_APP"],
    "slow_consumer": {"duration_sec": int(e["SWEEP_SLOW_DURATION"]), "producer_rate": int(e["SWEEP_SLOW_RATE"]),
                      "sleep_per_record_ms": 1} if e["SWEEP_CONSUMER_APP"] == "slow" else None,
    "broker_update_mode": e["SWEEP_BROKER_MODE"] or None,
    "topic": {"partitions": 12, "replication_factor": 3, "min.insync.replicas": 2},
}, open(sys.argv[1], "w"), indent=2)
EOF

"$KOT_ROOT/bench/environment.sh" "$out/environment.json"

# ---------------------------------------------------------------- state for cleanup
current_topic=""
sampler_pid=""
broker_applied=""

stop_sampler() {
  [[ -n "$sampler_pid" ]] || return 0
  local pid=$sampler_pid status=0
  # Prometheus returns the most recent scrape, which can predate the end of the
  # run by up to one scrape interval. Wait for one more scrape so the final
  # sample covers the whole run.
  sleep $((KOT_SCRAPE_INTERVAL + 1))
  sampler_pid=""
  kill -TERM "$pid" 2>/dev/null || true
  wait "$pid" || status=$?
  (( status == 0 )) || die "bench/sample.py exited with status $status. See the error above. Broker samples for this run are incomplete."
}

start_sampler() {
  local ready="$raw/.sampler-ready"
  rm -f "$ready"
  python3 "$KOT_ROOT/bench/sample.py" --prometheus "$KOT_PROMETHEUS_URL" --out "$out/broker-samples.csv" --ready-file "$ready" &
  sampler_pid=$!
  local waited=0
  while [[ ! -f "$ready" ]]; do
    kill -0 "$sampler_pid" 2>/dev/null || { local st=0; wait "$sampler_pid" || st=$?; sampler_pid=""; die "bench/sample.py failed to start (status $st)."; }
    (( waited < 150 )) || die "bench/sample.py did not take its first sample within 15 seconds."
    sleep 0.1; waited=$((waited + 1))
  done
}

topic_exists() {
  local topics
  topics=$(ktool kafka-topics.sh --bootstrap-server "$KOT_BOOTSTRAP" --list) || die "kafka-topics.sh --list failed."
  grep -qx "$1" <<<"$topics"
}

delete_topic() {
  local t=$1 waited=0
  ktool kafka-topics.sh --bootstrap-server "$KOT_BOOTSTRAP" --delete --topic "$t" >/dev/null
  while topic_exists "$t"; do
    (( waited < 60 )) || die "topic $t still exists 60 seconds after deletion."
    sleep 1; waited=$((waited + 1))
  done
}

# Waits for every partition of the topic to have a leader and all 3 replicas in sync.
wait_topic_ready() {
  local t=$1 waited=0 desc
  while true; do
    desc=$(ktool kafka-topics.sh --bootstrap-server "$KOT_BOOTSTRAP" --describe --topic "$t") || die "kafka-topics.sh --describe $t failed."
    [[ $(grep -cE 'Leader: [0-9]+.*Isr: [0-9]+,[0-9]+,[0-9]+' <<<"$desc") == 12 ]] && return 0
    (( waited < 60 )) || die "topic $t did not reach 12 partitions with a leader and 3 in-sync replicas within 60 seconds."
    sleep 1; waited=$((waited + 1))
  done
}

cleanup() {
  local status=$?
  set +e
  if [[ -n "$sampler_pid" ]]; then kill -TERM "$sampler_pid" 2>/dev/null; wait "$sampler_pid" 2>/dev/null; fi
  if [[ -n "$current_topic" ]]; then
    log "cleanup: deleting topic $current_topic"
    ktool kafka-topics.sh --bootstrap-server "$KOT_BOOTSTRAP" --delete --topic "$current_topic" >/dev/null 2>&1
  fi
  if [[ -n "$broker_applied" ]]; then
    log "cleanup: reverting broker setting $setting"
    revert_broker_setting
  fi
  if (( status != 0 )); then
    printf '\nSWEEP FAILED. %s is incomplete and keeps its INCOMPLETE marker. Delete it before running make report.\n' "${out#"$KOT_ROOT"/}" >&2
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# ---------------------------------------------------------------- broker settings
restart_brokers() {
  log "restarting brokers ${KOT_BROKERS[*]}"
  compose restart "${KOT_BROKERS[@]}" >/dev/null
  wait_for_cluster 180
}

# Waits until every broker reports the expected value for the setting.
verify_broker_setting() {
  local expect=$1 waited=0 id ok
  while true; do
    ok=0
    for id in "${KOT_BROKER_IDS[@]}"; do
      # Capture first: grep -q would close the pipe early and, with pipefail, fail the check.
      desc=$(ktool kafka-configs.sh --bootstrap-server "$KOT_BOOTSTRAP" --describe --entity-type brokers --entity-name "$id" --all) \
        || die "kafka-configs.sh --describe failed for broker $id."
      if grep -q "^  $setting=$expect sensitive=" <<<"$desc"; then
        ok=$((ok + 1))
      fi
    done
    (( ok == ${#KOT_BROKER_IDS[@]} )) && return 0
    (( waited < 30 )) || die "after 30 seconds only $ok of ${#KOT_BROKER_IDS[@]} brokers report $setting=$expect."
    sleep 2; waited=$((waited + 2))
  done
}

apply_broker_setting() {
  local value=$1 id
  case "$broker_mode" in
    cluster-wide)
      log "broker setting path: dynamic ($setting is cluster-wide). kafka-configs.sh --alter --entity-type brokers --entity-default --add-config $setting=$value"
      ktool kafka-configs.sh --bootstrap-server "$KOT_BOOTSTRAP" --alter --entity-type brokers --entity-default --add-config "$setting=$value" >/dev/null
      ;;
    per-broker)
      log "broker setting path: dynamic ($setting is per-broker). kafka-configs.sh --alter --entity-type brokers --entity-name <id> --add-config $setting=$value for each broker"
      for id in "${KOT_BROKER_IDS[@]}"; do
        ktool kafka-configs.sh --bootstrap-server "$KOT_BOOTSTRAP" --alter --entity-type brokers --entity-name "$id" --add-config "$setting=$value" >/dev/null
      done
      ;;
    read-only)
      log "broker setting path: static ($setting is read-only). Writing $setting=$value to cluster/overrides/broker.properties and restarting the brokers"
      printf '%s=%s\n' "$setting" "$value" > "$KOT_ROOT/cluster/overrides/broker.properties"
      broker_applied=yes
      restart_brokers
      ;;
  esac
  broker_applied=yes
  verify_broker_setting "$value"
  log "verified: all brokers report $setting=$value"
}

revert_broker_setting() {
  local id
  case "$broker_mode" in
    cluster-wide)
      ktool kafka-configs.sh --bootstrap-server "$KOT_BOOTSTRAP" --alter --entity-type brokers --entity-default --delete-config "$setting" >/dev/null ;;
    per-broker)
      for id in "${KOT_BROKER_IDS[@]}"; do
        ktool kafka-configs.sh --bootstrap-server "$KOT_BOOTSTRAP" --alter --entity-type brokers --entity-name "$id" --delete-config "$setting" >/dev/null
      done ;;
    read-only)
      rm -f "$KOT_ROOT/cluster/overrides/broker.properties"
      restart_brokers ;;
  esac
  broker_applied=""
  log "reverted $setting to its default on all brokers"
}

# ---------------------------------------------------------------- payloads
payload_args=()
producer_class=""
case "$payload" in
  letters) payload_args=(--record-size "$record_size") ;;
  json)
    mkdir -p "$KOT_ROOT/work"
    pfile="payload-json-$record_size.txt"
    python3 "$KOT_ROOT/bench/gen_json_payload.py" --record-size "$record_size" --lines 10000 --out "$KOT_ROOT/work/$pfile"
    kclient test -s "/work/$pfile" || die "the client container cannot see work/$pfile. Is ./work mounted at /work?"
    cp "$KOT_ROOT/work/$pfile" "$raw/$pfile"
    payload_args=(--payload-file "/work/$pfile")
    ;;
  random-bytes)
    payload_args=(--record-size "$record_size")
    producer_class=org.apache.kafka.tools.RandomBytesProducerPerformance
    ;;
esac

# Runs the producer perf test. Arguments: topic, num-records, throughput, then extra properties.
run_producer() {
  local topic=$1 n=$2 throughput=$3; shift 3
  local props=("bootstrap.servers=$KOT_BOOTSTRAP" "$@")
  local args=(--topic "$topic" --num-records "$n" "${payload_args[@]}" --throughput "$throughput" --print-metrics --command-property "${props[@]}")
  if [[ -n "$producer_class" ]]; then
    kclient env CLASSPATH=/opt/kot/classes KAFKA_HEAP_OPTS=-Xmx512M /opt/kafka/bin/kafka-run-class.sh "$producer_class" "${args[@]}"
  else
    ktool kafka-producer-perf-test.sh "${args[@]}"
  fi
}

# Properties for the producer runs of one value.
producer_props_for() {
  local value=$1 p=()
  [[ "$side" == producer && "$setting" == acks ]] || p+=("acks=all")
  [[ "$side" == producer ]] && p+=("$setting=$value")
  p+=("${fixed[@]+"${fixed[@]}"}")
  printf '%s\n' "${p[@]}"
}

now() { python3 -c 'import time; print("%.3f" % time.time())'; }

# ---------------------------------------------------------------- the sweep
for value in "${values[@]}"; do
  topic="bench-$setting-$value"
  log "=== $side $setting=$value (topic $topic)"

  if topic_exists "$topic"; then
    log "topic $topic is left over from an earlier run, deleting it first"
    delete_topic "$topic"
  fi
  ktool kafka-topics.sh --bootstrap-server "$KOT_BOOTSTRAP" --create --topic "$topic" \
    --partitions 12 --replication-factor 3 --config min.insync.replicas=2 >/dev/null
  current_topic=$topic
  wait_topic_ready "$topic"

  if [[ "$side" == broker ]]; then
    apply_broker_setting "$value"
    # A restart (read-only settings) leaves followers catching up. Wait again.
    wait_topic_ready "$topic"
  fi

  pprops=()
  while IFS= read -r line; do pprops+=("$line"); done < <(producer_props_for "$value")
  for run in $(seq 1 "$runs"); do
    kept=1; (( run == 1 )) && kept=0
    logf="$raw/producer-$value-run$run.log"
    log "producer run $run/$runs$([[ $kept == 0 ]] && echo ' (discarded: cold page cache and JIT)') properties: ${pprops[*]}"
    t0=$(now)
    start_sampler
    status=0
    run_producer "$topic" "$records" -1 "${pprops[@]}" > "$logf" 2>&1 || status=$?
    stop_sampler
    t1=$(now)
    (( status == 0 )) || { tail -20 "$logf" >&2; die "producer run $run for $setting=$value exited with status $status. Full log: ${logf#"$KOT_ROOT"/}"; }
    echo "$side,$setting,$value,producer,$run,$t0,$t1" >> "$out/phases.csv"
    row=$(python3 "$KOT_ROOT/bench/parse_perf.py" producer "$logf" --expect-records "$records") \
      || die "could not parse producer run $run for $setting=$value. Full log: ${logf#"$KOT_ROOT"/}"
    echo "$side,$setting,$value,$run,$kept,$row" >> "$out/runs.csv"
    metrics=$(python3 "$KOT_ROOT/bench/parse_perf.py" client-metrics "$logf" --client producer) \
      || die "could not read producer metrics for run $run of $setting=$value. Full log: ${logf#"$KOT_ROOT"/}"
    sed "s/^/$side,$setting,$value,$run,producer,/" <<<"$metrics" >> "$out/client-metrics.csv"
    log "producer run $run: $(grep -E 'records sent, .* 99\.9th\.$' "$logf")"
  done

  cprops=()
  [[ "$side" == consumer ]] && cprops=(--command-property "$setting=$value")
  logf="$raw/consumer-$value.log"
  log "consumer run (kafka-consumer-perf-test, $records records)${cprops[*]:+ with $setting=$value}"
  t0=$(now)
  start_sampler
  status=0
  ktool kafka-consumer-perf-test.sh --bootstrap-server "$KOT_BOOTSTRAP" --topic "$topic" --num-records "$records" \
    --group "kot-perf-$setting-$value-$stamp" --timeout 60000 --print-metrics "${cprops[@]+"${cprops[@]}"}" > "$logf" 2>&1 || status=$?
  stop_sampler
  t1=$(now)
  (( status == 0 )) || { tail -20 "$logf" >&2; die "consumer run for $setting=$value exited with status $status. Full log: ${logf#"$KOT_ROOT"/}"; }
  echo "$side,$setting,$value,consumer,1,$t0,$t1" >> "$out/phases.csv"
  row=$(python3 "$KOT_ROOT/bench/parse_perf.py" consumer "$logf" --expect-records "$records") \
    || die "could not parse the consumer run for $setting=$value. Full log: ${logf#"$KOT_ROOT"/}"
  echo "$side,$setting,$value,$row" >> "$out/consumer.csv"
  metrics=$(python3 "$KOT_ROOT/bench/parse_perf.py" client-metrics "$logf" --client consumer) \
    || die "could not read consumer metrics for $setting=$value. Full log: ${logf#"$KOT_ROOT"/}"
  sed "s/^/$side,$setting,$value,1,consumer,/" <<<"$metrics" >> "$out/client-metrics.csv"
  log "consumer run: $(awk -F, '{printf "%s records, %s records/sec, %s MB/sec", $5, $6, $4}' <<<"$row")"

  if [[ "$consumer_app" == slow ]]; then
    logf="$raw/slow-consumer-$value.log"
    plog="$raw/slow-producer-$value.log"
    log "slow consumer run: SlowConsumer for ${slow_duration}s, 1 ms per record, $setting=$value, with a producer at $slow_rate records/sec"
    t0=$(now)
    start_sampler
    # The producer runs a little longer than the consumer so the consumer never runs dry at the end.
    run_producer "$topic" $(( slow_rate * (slow_duration + 15) )) "$slow_rate" "acks=all" > "$plog" 2>&1 &
    ppid=$!
    status=0
    kclient java -cp '/opt/kot/classes:/opt/kafka/libs/*' kot.SlowConsumer --bootstrap-server "$KOT_BOOTSTRAP" \
      --topic "$topic" --group "kot-slow-$setting-$value-$stamp" --duration-sec "$slow_duration" \
      --sleep-per-record-ms 1 --command-property "$setting=$value" --command-property auto.offset.reset=latest \
      > "$logf" 2>&1 || status=$?
    pstatus=0
    wait "$ppid" || pstatus=$?
    stop_sampler
    t1=$(now)
    (( status == 0 )) || { tail -20 "$logf" >&2; die "SlowConsumer for $setting=$value exited with status $status. Full log: ${logf#"$KOT_ROOT"/}"; }
    (( pstatus == 0 )) || { tail -20 "$plog" >&2; die "the throttled producer for $setting=$value exited with status $pstatus. Full log: ${plog#"$KOT_ROOT"/}"; }
    echo "$side,$setting,$value,slow-consumer,1,$t0,$t1" >> "$out/phases.csv"
    row=$(python3 "$KOT_ROOT/bench/parse_perf.py" slow "$logf") \
      || die "could not parse SlowConsumer output for $setting=$value. Full log: ${logf#"$KOT_ROOT"/}"
    echo "$side,$setting,$value,$row,$slow_rate" >> "$out/slow-consumer.csv"
    log "slow consumer: $(grep '^SUMMARY' "$logf")"
  fi

  log "deleting topic $topic"
  delete_topic "$topic"
  current_topic=""
done

[[ "$side" == broker ]] && revert_broker_setting

rm -f "$out/INCOMPLETE" "$raw/.sampler-ready"
log "sweep complete: ${out#"$KOT_ROOT"/}"
