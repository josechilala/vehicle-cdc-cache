#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAMESPACE="vehicle-cache"
KUBE_CONTEXT="${KUBE_CONTEXT:-docker-desktop}"
KUBECTL=(kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE")
CANARY_ID="00000000-0000-4000-8000-000000000001"
VEHICLE_TOPIC="vehicle_cdc.public.vehicles"
VALIDATION_TIMEOUT_SECONDS=300

if [[ "${VALIDATION_TIMEOUT_WRAPPED:-}" != "1" ]]; then
  exec timeout --signal=TERM --kill-after=10s "${VALIDATION_TIMEOUT_SECONDS}s" \
    env VALIDATION_TIMEOUT_WRAPPED=1 bash "$0" "$@"
fi

cd "$ROOT_DIR"

wal_level="$("${KUBECTL[@]}" exec postgres-0 -- psql -U postgres -d vehicle_cache -Atc 'SHOW wal_level')"
if [[ "$wal_level" != "logical" ]]; then
  echo "PostgreSQL wal_level is '$wal_level'; expected 'logical'." >&2
  exit 1
fi

tables="$("${KUBECTL[@]}" exec postgres-0 -- psql -U postgres -d vehicle_cache -Atc \
  "SELECT to_regclass('public.vehicles') IS NOT NULL AND to_regclass('public.positions') IS NOT NULL")"
if [[ "$tables" != "t" ]]; then
  echo "Expected PostgreSQL tables public.vehicles and public.positions." >&2
  exit 1
fi

publication="$("${KUBECTL[@]}" exec postgres-0 -- psql -U postgres -d vehicle_cache -Atc \
  "SELECT count(*) FROM pg_publication_tables WHERE pubname = 'vehicle_cache_publication'")"
if [[ "$publication" != "2" ]]; then
  echo "Expected the Debezium publication to include exactly two source tables." >&2
  exit 1
fi

for topic in vehicle_cdc.public.vehicles vehicle_cdc.public.positions; do
  description="$("${KUBECTL[@]}" exec kafka-0 -- \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic "$topic")"
  if [[ "$description" != *"PartitionCount: 3"* ||
    "$description" != *"cleanup.policy=delete"* ||
    "$description" != *"retention.ms=-1"* ||
    "$description" != *"retention.bytes=-1"* ]]; then
    echo "Kafka topic '$topic' must have three partitions, delete cleanup, and unlimited retention." >&2
    exit 1
  fi
done

transaction_topic="$("${KUBECTL[@]}" exec kafka-0 -- \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
    --describe --topic vehicle_cdc.transaction)"
if [[ "$transaction_topic" != *"PartitionCount: 1"* ]]; then
  echo "Debezium transaction metadata topic must have one partition." >&2
  exit 1
fi

valkey_response="$("${KUBECTL[@]}" exec deployment/valkey -- valkey-cli ping)"
if [[ "$valkey_response" != "PONG" ]]; then
  echo "Valkey did not return PONG." >&2
  exit 1
fi

read_vehicle_records() {
  local offsets topic partition end_offset records
  offsets="$("${KUBECTL[@]}" exec kafka-0 -- \
    /opt/kafka/bin/kafka-get-offsets.sh \
      --bootstrap-server kafka:9092 \
      --topic "$VEHICLE_TOPIC" \
      --time -1)"
  while IFS=: read -r topic partition end_offset; do
    [[ "$topic" == "$VEHICLE_TOPIC" && "$end_offset" =~ ^[0-9]+$ ]] || continue
    ((end_offset > 0)) || continue
    records="$("${KUBECTL[@]}" exec kafka-0 -- \
      /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server kafka:9092 \
        --topic "$VEHICLE_TOPIC" \
        --partition "$partition" \
        --offset 0 \
        --max-messages "$end_offset" \
        --timeout-ms 2000 2>/dev/null || true)"
    printf '%s\n' "$records"
  done <<<"$offsets"
}

health_job="connector-health-${BASHPID}-${RANDOM}"
if ! timeout --signal=TERM --kill-after=5s 30s \
  kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
    create job "$health_job" --image=curlimages/curl:8.12.1 -- \
    sh -ec 'curl --fail --silent --show-error http://kafka-connect:8083/connectors/vehicle-cdc-connector/status'; then
  echo "Could not create the Debezium connector health check Job." >&2
  exit 1
fi
if ! timeout --signal=TERM --kill-after=5s 90s \
  kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
    wait --for=condition=complete "job/$health_job" --timeout=75s; then
  health_logs="$(kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
    logs "job/$health_job" 2>&1 || true)"
  kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
    delete job "$health_job" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  printf 'Debezium connector health check Job failed: %s\n' "$health_logs" >&2
  exit 1
fi
if ! connector_status="$(timeout --signal=TERM --kill-after=5s 30s \
  kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
    logs "job/$health_job")"; then
  kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
    delete job "$health_job" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  echo "Could not read the Debezium connector health check result." >&2
  exit 1
fi
kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
  delete job "$health_job" --ignore-not-found --wait=false >/dev/null
running_count="$(printf '%s' "$connector_status" |
  grep -o '"state"[[:space:]]*:[[:space:]]*"RUNNING"' |
  wc -l || true)"
if ((running_count < 2)); then
  echo "Debezium connector or task is not RUNNING: $connector_status" >&2
  exit 1
fi

snapshot_seen="$("${KUBECTL[@]}" get namespace "$NAMESPACE" \
  -o 'jsonpath={.metadata.annotations.vehicle-cache\.dev/snapshot-ready}')"
if [[ "$snapshot_seen" != "true" ]]; then
  echo "Waiting for Debezium to publish the canary snapshot and streaming transition..."
  for attempt in $(seq 1 60); do
    snapshot_records="$(read_vehicle_records)"
    if printf '%s\n' "$snapshot_records" | grep -Fq "$CANARY_ID" &&
      printf '%s\n' "$snapshot_records" | grep -Fq '"op":"r"' &&
      printf '%s\n' "$snapshot_records" | grep -Fq '"state_version":1'; then
      break
    fi
    if ((attempt == 60)); then
      echo "Debezium initial snapshot did not publish the version-1 canary row." >&2
      exit 1
    fi
  done

  "${KUBECTL[@]}" exec postgres-0 -- psql -v ON_ERROR_STOP=1 \
    -U postgres -d vehicle_cache -c \
    "DO \$\$
     BEGIN
       UPDATE public.vehicles
          SET state = 'ACTIVE',
              state_version = 2,
              state_updated_at = TIMESTAMPTZ '2026-01-01 00:00:02+00'
        WHERE id = '${CANARY_ID}'
          AND state = 'REGISTERED'
          AND state_version = 1;
       IF NOT EXISTS (
         SELECT 1
           FROM public.vehicles
          WHERE id = '${CANARY_ID}'
            AND state = 'ACTIVE'
            AND state_version = 2
            AND state_updated_at = TIMESTAMPTZ '2026-01-01 00:00:02+00'
       ) THEN
         RAISE EXCEPTION 'Canary row is not in the expected version-2 ACTIVE state';
       END IF;
     END
     \$\$;"

  stream_seen=false
  for attempt in $(seq 1 60); do
    stream_records="$(read_vehicle_records)"
    if printf '%s\n' "$stream_records" | grep -Fq "$CANARY_ID" &&
      printf '%s\n' "$stream_records" | grep -Fq '"op":"u"' &&
      printf '%s\n' "$stream_records" | grep -Fq '"state_version":2' &&
      printf '%s\n' "$stream_records" | grep -Fq '"state":"ACTIVE"'; then
      stream_seen=true
      break
    fi
  done
  if [[ "$stream_seen" != true ]]; then
    echo "Post-snapshot PostgreSQL update was not observed as a Debezium CDC event." >&2
    exit 1
  fi

  kubectl --context "$KUBE_CONTEXT" annotate namespace "$NAMESPACE" \
    vehicle-cache.dev/snapshot-ready=true --overwrite
fi

echo "Infrastructure validation passed: PostgreSQL, Kafka, Debezium snapshot/stream, and Valkey."
