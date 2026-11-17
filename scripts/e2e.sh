#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAMESPACE="vehicle-cache"
KUBE_CONTEXT="${KUBE_CONTEXT:-docker-desktop}"
E2E_GROUP="vehicle-cache-e2e"
DUPLICATE_REPLAY_GROUP="${E2E_GROUP}-duplicate-replay"
FULL_REPLAY_GROUP="${E2E_GROUP}-full-replay"
KAFKA_GROUP="$E2E_GROUP"
VEHICLE_TOPIC="vehicle_cdc.public.vehicles"
POSITION_TOPIC="vehicle_cdc.public.positions"
POSTGRES_PORT=15432
KAFKA_PORT=19092
VALKEY_PORT=16379
WORKER_DLL="$ROOT_DIR/src/VehicleCache.Worker/bin/Release/net10.0/VehicleCache.Worker.dll"
VEHICLE_WRITER_DLL="$ROOT_DIR/src/VehicleWriter/bin/Release/net10.0/VehicleWriter.dll"
POSITION_WRITER_DLL="$ROOT_DIR/src/PositionWriter/bin/Release/net10.0/PositionWriter.dll"
TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/vehicle-cache-e2e.XXXXXX")"
WORKER_PID=""
POSTGRES_FORWARD_PID=""
KAFKA_FORWARD_PID=""
VALKEY_FORWARD_PID=""
declare -A END_OFFSETS=()

kube() {
  timeout --signal=TERM --kill-after=5s 60s \
    kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" "$@"
}

kafka_exec() {
  kube exec kafka-0 -- "/opt/kafka/bin/$1" "${@:2}"
}

valkey_cli() {
  kube exec deployment/valkey -- valkey-cli "$@"
}

fail() {
  printf 'E2E validation failed: %s\n' "$*" >&2
  exit 1
}

stop_process() {
  local process_id="$1"
  local attempt
  [[ -n "$process_id" ]] || return 0
  if kill -0 "$process_id" 2>/dev/null; then
    kill -TERM "$process_id" 2>/dev/null || true
    for ((attempt = 0; attempt < 15; attempt++)); do
      kill -0 "$process_id" 2>/dev/null || break
      sleep 1
    done
    if kill -0 "$process_id" 2>/dev/null; then
      kill -KILL "$process_id" 2>/dev/null || true
    fi
  fi
  wait "$process_id" 2>/dev/null || true
}

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM
  if ((exit_code != 0)) && [[ -s "$TEMP_DIR/worker.log" ]]; then
    printf '\nLast Worker output:\n' >&2
    tail -n 80 "$TEMP_DIR/worker.log" >&2 || true
  fi
  stop_process "$WORKER_PID"
  stop_process "$POSTGRES_FORWARD_PID"
  stop_process "$KAFKA_FORWARD_PID"
  stop_process "$VALKEY_FORWARD_PID"
  rm -rf -- "$TEMP_DIR"
  exit "$exit_code"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for command_name in dotnet timeout sort cmp diff base64 grep tail; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    fail "required command '$command_name' is unavailable in the devenv shell."
  fi
done

check_local_port() {
  local port="$1"
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    fail "local port $port is already in use; no Kubernetes namespace was changed."
  fi
}

check_local_port "$POSTGRES_PORT"
check_local_port "$KAFKA_PORT"
check_local_port "$VALKEY_PORT"

cd "$ROOT_DIR"
bash scripts/check-prerequisites.sh
printf 'Building the solution for the end-to-end run...\n'
timeout --signal=TERM --kill-after=10s 600s \
  dotnet build VehicleCache.slnx --configuration Release --nologo -m:1

printf 'Resetting only namespace %s in context %s for a clean deterministic run...\n' \
  "$NAMESPACE" "$KUBE_CONTEXT"
timeout --signal=TERM --kill-after=10s 240s bash scripts/down.sh
timeout --signal=TERM --kill-after=10s 1500s bash scripts/up.sh

printf 'Starting temporary local port-forwards...\n'
timeout --signal=TERM --kill-after=5s 3600s \
  kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
  port-forward --address 127.0.0.1 service/postgres "$POSTGRES_PORT:5432" \
  >"$TEMP_DIR/postgres-forward.log" 2>&1 &
POSTGRES_FORWARD_PID=$!
timeout --signal=TERM --kill-after=5s 3600s \
  kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
  port-forward --address 127.0.0.1 service/kafka "$KAFKA_PORT:19092" \
  >"$TEMP_DIR/kafka-forward.log" 2>&1 &
KAFKA_FORWARD_PID=$!
timeout --signal=TERM --kill-after=5s 3600s \
  kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
  port-forward --address 127.0.0.1 service/valkey "$VALKEY_PORT:6379" \
  >"$TEMP_DIR/valkey-forward.log" 2>&1 &
VALKEY_FORWARD_PID=$!

wait_for_forward() {
  local name="$1"
  local process_id="$2"
  local port="$3"
  local log_file="$4"
  for ((attempt = 0; attempt < 60; attempt++)); do
    if grep -Fq "Forwarding from 127.0.0.1:$port ->" "$log_file"; then
      return 0
    fi
    if ! kill -0 "$process_id" 2>/dev/null; then
      cat "$log_file" >&2
      fail "$name port-forward exited before becoming ready."
    fi
    sleep 1
  done
  cat "$log_file" >&2
  fail "$name port-forward did not become ready within 60 seconds."
}

wait_for_forward postgres "$POSTGRES_FORWARD_PID" "$POSTGRES_PORT" "$TEMP_DIR/postgres-forward.log"
wait_for_forward kafka "$KAFKA_FORWARD_PID" "$KAFKA_PORT" "$TEMP_DIR/kafka-forward.log"
wait_for_forward valkey "$VALKEY_FORWARD_PID" "$VALKEY_PORT" "$TEMP_DIR/valkey-forward.log"

WRITER_PASSWORD="$(
  kube get secret postgres-credentials \
    -o 'jsonpath={.data.WRITER_PASSWORD}' | base64 --decode
)"
if [[ -z "$WRITER_PASSWORD" ]]; then
  fail "the PostgreSQL writer password is missing from the project Secret."
fi
VEHICLE_CACHE_CONNECTION_STRING="Host=127.0.0.1;Port=$POSTGRES_PORT;Database=vehicle_cache;Username=vehicle_writer;Password=$WRITER_PASSWORD"

run_writer() {
  local writer_dll="$1"
  shift
  if timeout --signal=TERM --kill-after=5s 120s \
    env VEHICLE_CACHE_CONNECTION_STRING="$VEHICLE_CACHE_CONNECTION_STRING" \
    dotnet "$writer_dll" "$@"; then
    return 0
  else
    local exit_code=$?
    fail "deterministic writer '$writer_dll' failed or exceeded 120 seconds (exit code $exit_code)."
  fi
}

hash_field() {
  local value
  if ! value="$(valkey_cli --raw HGET "$1" "$2")"; then
    fail "could not read Valkey field '$2' from '$1'."
  fi
  printf '%s' "$value"
}

wait_for_hash_field() {
  local key="$1"
  local field="$2"
  local expected="$3"
  local description="$4"
  for ((attempt = 0; attempt < 180; attempt++)); do
    if [[ "$(hash_field "$key" "$field")" == "$expected" ]]; then
      return 0
    fi
    if [[ -n "$WORKER_PID" ]] && ! kill -0 "$WORKER_PID" 2>/dev/null; then
      fail "Worker exited while waiting for $description."
    fi
    sleep 1
  done
  fail "timed out waiting for $description."
}

start_worker() {
  : >"$TEMP_DIR/worker.log"
  env \
    Kafka__BootstrapServers="127.0.0.1:$KAFKA_PORT" \
    Kafka__ConsumerGroup="$KAFKA_GROUP" \
    Valkey__ConnectionString="127.0.0.1:$VALKEY_PORT" \
    Logging__LogLevel__VehicleCache.Worker.Consumers.CdcWorker=Debug \
    dotnet "$WORKER_DLL" >"$TEMP_DIR/worker.log" 2>&1 &
  WORKER_PID=$!

  for ((attempt = 0; attempt < 60; attempt++)); do
    if grep -Fq "Kafka replay baseline verified." "$TEMP_DIR/worker.log"; then
      return 0
    fi
    if ! kill -0 "$WORKER_PID" 2>/dev/null; then
      tail -n 80 "$TEMP_DIR/worker.log" >&2 || true
      fail "Worker exited before reporting startup."
    fi
    sleep 1
  done
  tail -n 80 "$TEMP_DIR/worker.log" >&2 || true
  fail "Worker did not validate Kafka topic baselines within 60 seconds."
}

stop_worker() {
  local process_id="$WORKER_PID"
  local attempt
  [[ -n "$process_id" ]] || return 0
  if kill -0 "$process_id" 2>/dev/null; then
    kill -TERM "$process_id"
    for ((attempt = 0; attempt < 20; attempt++)); do
      kill -0 "$process_id" 2>/dev/null || break
      sleep 1
    done
    if kill -0 "$process_id" 2>/dev/null; then
      kill -KILL "$process_id" 2>/dev/null || true
      wait "$process_id" 2>/dev/null || true
      fail "Worker did not stop within 20 seconds."
    fi
  fi
  WORKER_PID=""
  if ! wait "$process_id"; then
    fail "Worker failed while stopping cleanly."
  fi
}

assert_retained_baseline() {
  local topic="$1" offsets record_topic partition offset count=0
  local -A partitions=()
  offsets="$(kafka_exec kafka-get-offsets.sh \
    --bootstrap-server kafka:9092 --topic "$topic" --time -2)"
  while IFS=: read -r record_topic partition offset; do
    [[ "$record_topic" == "$topic" ]] || continue
    if [[ "$offset" != "0" ]]; then
      fail "topic '$topic' has an unexpected beginning offset or partition."
    fi
    case "$partition" in
      0|1|2) ;;
      *) fail "topic '$topic' has an unexpected beginning offset or partition." ;;
    esac
    partitions["$partition"]=1
    count=$((count + 1))
  done <<<"$offsets"
  if ((count != 3 || ${#partitions[@]} != 3)); then
    fail "topic '$topic' no longer retains offset 0 in all three partitions; replay was not started."
  fi
}

capture_end_offsets() {
  local topic="$1" output record_topic partition offset count=0
  output="$(kafka_exec kafka-get-offsets.sh \
    --bootstrap-server kafka:9092 --topic "$topic" --time -1)"
  while IFS=: read -r record_topic partition offset; do
    [[ "$record_topic" == "$topic" ]] || continue
    if [[ "$partition" != "0" && "$partition" != "1" && "$partition" != "2" ]] ||
      [[ ! "$offset" =~ ^[0-9]+$ ]]; then
      fail "topic '$topic' returned an invalid end offset."
    fi
    END_OFFSETS["$topic|$partition"]="$offset"
    printf '%s|%s|%s\n' "$topic" "$partition" "$offset" >>"$TEMP_DIR/end-offsets.txt"
    count=$((count + 1))
  done <<<"$output"
  if ((count != 3)); then
    fail "could not read all three end offsets for topic '$topic'."
  fi
}

progress_reached_end() {
  local topic partition expected actual
  for topic in "$VEHICLE_TOPIC" "$POSITION_TOPIC"; do
    for partition in 0 1 2; do
      expected="${END_OFFSETS["$topic|$partition"]-}"
      [[ -n "$expected" ]] || return 1
      actual="$(hash_field "vehicle-cache:progress:$KAFKA_GROUP" "$topic|$partition")"
      [[ -n "$actual" ]] || actual=0
      [[ "$actual" == "$expected" ]] || return 1
    done
  done
}

wait_for_replay_completion() {
  local timeout_seconds="$1"
  for ((attempt = 0; attempt < timeout_seconds; attempt++)); do
    if progress_reached_end; then
      return 0
    fi
    if [[ -n "$WORKER_PID" ]] && ! kill -0 "$WORKER_PID" 2>/dev/null; then
      fail "Worker exited before consuming all retained CDC records."
    fi
    sleep 1
  done
  fail "Worker did not consume all retained CDC records within ${timeout_seconds} seconds."
}

expected_projection_ready() {
  [[ "$(hash_field "vehicle:{10000000-0000-4000-8000-000000000001}" state)" == "IDLE" ]] &&
    [[ "$(hash_field "vehicle:{10000000-0000-4000-8000-000000000001}" state_version)" == "3" ]] &&
    [[ "$(hash_field "vehicle:{10000000-0000-4000-8000-000000000001}" position_id)" == "20000000-0000-4000-8000-000000000020" ]] &&
    [[ "$(hash_field "vehicle:{10000000-0000-4000-8000-000000000002}" is_deleted)" == "1" ]] &&
    [[ "$(hash_field "vehicle:{10000000-0000-4000-8000-000000000003}" state)" == "REGISTERED" ]] &&
    [[ "$(hash_field "vehicle:{10000000-0000-4000-8000-000000000003}" position_id)" == "20000000-0000-4000-8000-000000000030" ]]
}

wait_for_final_cdc() {
  local timeout_seconds="$1"
  local previous_offsets="" current_offsets="" stable_checks=0
  for ((attempt = 0; attempt < timeout_seconds / 2; attempt++)); do
    : >"$TEMP_DIR/end-offsets.txt"
    END_OFFSETS=()
    capture_end_offsets "$VEHICLE_TOPIC"
    capture_end_offsets "$POSITION_TOPIC"
    current_offsets="$(<"$TEMP_DIR/end-offsets.txt")"

    if [[ "$current_offsets" == "$previous_offsets" ]]; then
      stable_checks=$((stable_checks + 1))
    else
      stable_checks=0
    fi
    previous_offsets="$current_offsets"

    if ((stable_checks >= 4)) &&
      expected_projection_ready &&
      grep -Fq "Kafka tombstone recorded" "$TEMP_DIR/worker.log" &&
      progress_reached_end; then
      return 0
    fi
    if [[ -n "$WORKER_PID" ]] && ! kill -0 "$WORKER_PID" 2>/dev/null; then
      fail "Worker exited before all deterministic CDC was projected."
    fi
    sleep 2
  done
  fail "CDC, expected projection, and stable Kafka end offsets did not converge within ${timeout_seconds} seconds."
}

assert_field() {
  local key="$1"
  local field="$2"
  local expected="$3"
  local actual
  actual="$(hash_field "$key" "$field")"
  if [[ "$actual" != "$expected" ]]; then
    fail "$key field '$field' was '$actual'; expected '$expected'."
  fi
}

assert_field_absent() {
  local key="$1"
  local field="$2"
  local exists
  exists="$(valkey_cli --raw HEXISTS "$key" "$field")"
  if [[ "$exists" != "0" ]]; then
    fail "$key must not expose field '$field'."
  fi
}

assert_expected_projection() {
  local current="vehicle:{10000000-0000-4000-8000-000000000001}"
  local deleted="vehicle:{10000000-0000-4000-8000-000000000002}"
  local position_first="vehicle:{10000000-0000-4000-8000-000000000003}"
  local canary="vehicle:{00000000-0000-4000-8000-000000000001}"

  assert_field "$current" state IDLE
  assert_field "$current" vehicle_seen 1
  assert_field "$current" is_deleted 0
  assert_field "$current" state_present 1
  assert_field "$current" position_seen 1
  assert_field "$current" state_version 3
  assert_field "$current" state_updated_at 2026-01-02T11:59:00.000000Z
  assert_field "$current" position_id 20000000-0000-4000-8000-000000000020
  assert_field "$current" lat 40.7138
  assert_field "$current" lng -74.005
  assert_field "$current" recorded_at 2026-01-02T12:00:00.123457Z

  assert_field "$deleted" vehicle_seen 1
  assert_field "$deleted" is_deleted 1
  assert_field "$deleted" state_present 0
  assert_field "$deleted" position_seen 0
  assert_field "$deleted" delete_state_version 2
  for field in state state_version state_updated_at position_id lat lng recorded_at; do
    assert_field_absent "$deleted" "$field"
  done

  assert_field "$position_first" state REGISTERED
  assert_field "$position_first" vehicle_seen 1
  assert_field "$position_first" is_deleted 0
  assert_field "$position_first" state_present 1
  assert_field "$position_first" position_seen 1
  assert_field "$position_first" state_version 1
  assert_field "$position_first" position_id 20000000-0000-4000-8000-000000000030
  assert_field "$position_first" lat 34.0522
  assert_field "$position_first" lng -118.2437
  assert_field "$position_first" recorded_at 2026-01-02T12:00:00.123456Z

  assert_field "$canary" state ACTIVE
  assert_field "$canary" vehicle_seen 1
  assert_field "$canary" is_deleted 0
  assert_field "$canary" state_present 1
  assert_field "$canary" state_version 2
  assert_field "$canary" state_updated_at 2026-01-01T00:00:02.000000Z
}

dump_cache() {
  local key index keys_output rows_output
  local -a keys rows
  keys_output="$(valkey_cli --raw --scan --pattern 'vehicle:*')"
  mapfile -t keys < <(printf '%s\n' "$keys_output" | LC_ALL=C sort)
  if ((${#keys[@]} == 0)); then
    fail "Valkey contains no projected vehicle hashes."
  fi

  : >"$1"
  for key in "${keys[@]}"; do
    printf 'KEY=%s\n' "$key" >>"$1"
    rows_output="$(valkey_cli --raw HGETALL "$key")"
    mapfile -t rows <<<"$rows_output"
    if [[ -z "$rows_output" ]] || ((${#rows[@]} % 2 != 0)); then
      fail "Valkey hash '$key' returned an incomplete field/value pair."
    fi
    for ((index = 0; index < ${#rows[@]}; index += 2)); do
      printf '%s=%s\n' "${rows[index]}" "${rows[index + 1]}"
    done | LC_ALL=C sort >>"$1"
  done
}

clear_projection() {
  local keys key
  keys="$(valkey_cli --raw --scan --pattern 'vehicle:*')"
  while IFS= read -r key; do
    [[ -n "$key" ]] || continue
    valkey_cli DEL "$key" >/dev/null
  done <<<"$keys"
  valkey_cli DEL "vehicle-cache:progress:$KAFKA_GROUP" >/dev/null
}

printf 'Writing deterministic positions before vehicle creation...\n'
run_writer "$POSITION_WRITER_DLL" \
  --mode deterministic --scenario before-vehicles --rate 100

printf 'Starting Worker and waiting for position-before-vehicle projection...\n'
start_worker
wait_for_hash_field \
  "vehicle:{10000000-0000-4000-8000-000000000001}" \
  position_id \
  20000000-0000-4000-8000-000000000020 \
  "the latest deterministic position for the not-yet-created vehicle"
wait_for_hash_field \
  "vehicle:{10000000-0000-4000-8000-000000000003}" \
  position_id \
  20000000-0000-4000-8000-000000000030 \
  "the position for the second not-yet-created vehicle"

printf 'Restarting Worker between position and vehicle CDC...\n'
stop_worker
start_worker

printf 'Writing deterministic vehicle transitions and post-delete position...\n'
run_writer "$VEHICLE_WRITER_DLL" --mode deterministic --rate 100
run_writer "$POSITION_WRITER_DLL" \
  --mode deterministic --scenario after-delete --rate 100

printf 'Waiting for CDC publication, expected projection, and stable topic end offsets...\n'
wait_for_final_cdc 300
assert_expected_projection
if ! grep -Fq "Kafka tombstone recorded" "$TEMP_DIR/worker.log"; then
  fail "Worker did not report consuming the Debezium null tombstone."
fi
dump_cache "$TEMP_DIR/cache-before-replay.txt"

printf 'Replaying from the retained baseline with a fresh group to verify duplicate idempotency...\n'
stop_worker
KAFKA_GROUP="$DUPLICATE_REPLAY_GROUP"
start_worker
wait_for_replay_completion 300
dump_cache "$TEMP_DIR/cache-after-duplicates.txt"
if ! cmp -s "$TEMP_DIR/cache-before-replay.txt" "$TEMP_DIR/cache-after-duplicates.txt"; then
  diff -u "$TEMP_DIR/cache-before-replay.txt" "$TEMP_DIR/cache-after-duplicates.txt" >&2 || true
  fail "re-delivering CDC records changed the existing cache."
fi

printf 'Verifying retained baselines before clearing the projection for full replay...\n'
stop_worker
assert_retained_baseline "$VEHICLE_TOPIC"
assert_retained_baseline "$POSITION_TOPIC"
KAFKA_GROUP="$FULL_REPLAY_GROUP"
clear_projection

printf 'Rebuilding the projection from offset 0...\n'
start_worker
wait_for_replay_completion 300
assert_expected_projection
if ! grep -Fq "Kafka tombstone recorded" "$TEMP_DIR/worker.log"; then
  fail "full replay did not consume the Debezium null tombstone."
fi
dump_cache "$TEMP_DIR/cache-after-full-replay.txt"
if ! cmp -s "$TEMP_DIR/cache-before-replay.txt" "$TEMP_DIR/cache-after-full-replay.txt"; then
  diff -u "$TEMP_DIR/cache-before-replay.txt" "$TEMP_DIR/cache-after-full-replay.txt" >&2 || true
  fail "full replay from offset 0 did not rebuild the identical cache."
fi

stop_worker
printf 'End-to-end validation passed: stale events, duplicates, delete/tombstone, position-before-vehicle, restart, and identical full replay.\n'
