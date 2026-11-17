#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAMESPACE="vehicle-cache"
KUBE_CONTEXT="${KUBE_CONTEXT:-docker-desktop}"
STARTUP_TIMEOUT_SECONDS=1200

if [[ "${UP_TIMEOUT_WRAPPED:-}" != "1" ]]; then
  exec timeout --signal=TERM --kill-after=10s "${STARTUP_TIMEOUT_SECONDS}s" \
    env UP_TIMEOUT_WRAPPED=1 bash "$0" "$@"
fi

cd "$ROOT_DIR"
bash scripts/check-prerequisites.sh

if ! kubectl config get-contexts -o name | grep -Fxq "$KUBE_CONTEXT"; then
  echo "Kubernetes context '${KUBE_CONTEXT}' is not configured." >&2
  exit 1
fi

kubectl --context "$KUBE_CONTEXT" apply -f infrastructure/kubernetes/namespace.yaml
if ! kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
  get secret postgres-credentials >/dev/null 2>&1; then
  kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
    create secret generic postgres-credentials \
    --from-literal="POSTGRES_PASSWORD=$(openssl rand -hex 32)" \
    --from-literal="DEBEZIUM_PASSWORD=$(openssl rand -hex 32)" \
    --from-literal="WRITER_PASSWORD=$(openssl rand -hex 32)"
fi
kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
  delete job connector-registration --ignore-not-found --wait=true --timeout=30s

startup_deadline=$((SECONDS + STARTUP_TIMEOUT_SECONDS))

remaining_startup_seconds() {
  local remaining=$((startup_deadline - SECONDS))
  if ((remaining <= 0)); then
    return 1
  fi
  printf '%s' "$remaining"
}

report_startup_failure() {
  local reason="$1"
  printf 'Infrastructure startup failed: %s\n' "$reason" >&2
  printf '\nProject pods in context %s, namespace %s:\n' \
    "$KUBE_CONTEXT" "$NAMESPACE" >&2
  kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
    get pods -o wide >&2 || true
  printf '\nRecent namespace events:\n' >&2
  kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
    get events --sort-by=.lastTimestamp | tail -n 40 >&2 || true
  for app_name in postgres kafka kafka-connect valkey; do
    printf '\nRecent %s container logs:\n' "$app_name" >&2
    kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
      logs --selector="app=$app_name" --all-containers=true --tail=60 >&2 || true
  done
  printf '\nConnector registration Job logs:\n' >&2
  kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
    logs --selector=app=connector-registration \
      --all-containers=true --prefix=true --tail=120 >&2 || true
  exit 1
}

wait_for_rollout() {
  local resource="$1"
  local remaining
  if ! remaining="$(remaining_startup_seconds)"; then
    report_startup_failure "the ${STARTUP_TIMEOUT_SECONDS}-second startup deadline expired before ${resource} became ready."
  fi
  printf 'Waiting up to %s seconds for %s readiness...\n' "$remaining" "$resource"
  if ! kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
    rollout status "$resource" --timeout="${remaining}s"; then
    report_startup_failure "$resource did not become ready within the remaining startup deadline."
  fi
}

refresh_stale_statefulset_pod() {
  local resource="$1"
  local pod_name="$2"
  local desired_revision
  local pod_revision
  local remaining

  if ! desired_revision="$(
    kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
      get "$resource" -o 'jsonpath={.status.updateRevision}'
  )"; then
    report_startup_failure "could not read the target revision for ${resource}."
  fi
  if ! pod_revision="$(
    kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
      get pod "$pod_name" --ignore-not-found \
      -o 'jsonpath={.metadata.labels.controller-revision-hash}'
  )"; then
    report_startup_failure "could not inspect StatefulSet pod ${pod_name}."
  fi
  if [[ -z "$pod_revision" || -z "$desired_revision" ||
    "$pod_revision" == "$desired_revision" ]]; then
    return 0
  fi

  remaining="$(remaining_startup_seconds)" ||
    report_startup_failure "startup deadline expired before replacing stale pod ${pod_name}."
  printf 'Replacing stale StatefulSet pod %s without deleting its volume...\n' "$pod_name"
  if ! kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
    delete pod "$pod_name" --ignore-not-found=true --wait=true --timeout="${remaining}s"; then
    report_startup_failure "stale StatefulSet pod ${pod_name} could not be replaced."
  fi
}

remaining="$(remaining_startup_seconds)" ||
  report_startup_failure "startup deadline expired before DevSpace deployment."
printf 'Deploying project workloads with DevSpace (startup deadline: %s seconds)...\n' \
  "$remaining"
if ! timeout --signal=TERM --kill-after=10s "${remaining}s" \
  devspace deploy \
    --config devspace.yaml \
    --namespace "$NAMESPACE" \
    --kube-context "$KUBE_CONTEXT"; then
  report_startup_failure "DevSpace deployment failed or exceeded the startup deadline."
fi

refresh_stale_statefulset_pod statefulset/postgres postgres-0
refresh_stale_statefulset_pod statefulset/kafka kafka-0

wait_for_rollout statefulset/postgres
printf 'Ensuring PostgreSQL database, schema, roles, and publication...\n'
remaining="$(remaining_startup_seconds)" ||
  report_startup_failure "startup deadline expired before PostgreSQL initialization."
if ! timeout --signal=TERM --kill-after=10s "${remaining}s" \
  kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
    exec postgres-0 -- /docker-entrypoint-initdb.d/10-init.sh; then
  report_startup_failure "PostgreSQL database/schema initialization failed."
fi
postgres_schema="$(
  kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
    exec postgres-0 -- psql -v ON_ERROR_STOP=1 -U postgres -d vehicle_cache -Atc \
    "SELECT to_regclass('public.vehicles') IS NOT NULL
      AND to_regclass('public.positions') IS NOT NULL
      AND (SELECT count(*) FROM pg_publication_tables
           WHERE pubname = 'vehicle_cache_publication') = 2"
)" || report_startup_failure "PostgreSQL schema verification failed."
if [[ "$postgres_schema" != "t" ]]; then
  report_startup_failure "PostgreSQL tables or Debezium publication are incomplete."
fi

wait_for_rollout statefulset/kafka
wait_for_rollout deployment/valkey
wait_for_rollout deployment/kafka-connect

printf 'PostgreSQL, Kafka, Kafka Connect, and Valkey are ready; registering the Debezium connector.\n'
remaining="$(remaining_startup_seconds)" ||
  report_startup_failure "startup deadline expired before connector registration."
if ! timeout --signal=TERM --kill-after=10s "${remaining}s" \
  kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
    apply -f infrastructure/registration/connector-registration.yaml; then
  report_startup_failure "could not create the Debezium connector-registration Job."
fi

while true; do
  remaining="$(remaining_startup_seconds)" ||
    report_startup_failure "startup deadline expired while waiting for connector registration."
  if ! job_conditions="$(
    kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
      get job connector-registration \
      -o 'jsonpath={range .status.conditions[*]}{.type}={.status}{"\n"}{end}'
  )"; then
    report_startup_failure "could not read the Debezium connector-registration Job status."
  fi
  if grep -Eq '^(FailureTarget|Failed)=True$' <<<"$job_conditions"; then
    report_startup_failure "the Debezium connector-registration Job failed."
  fi
  if grep -Fxq 'Complete=True' <<<"$job_conditions"; then
    break
  fi
  sleep 2
done

remaining="$(remaining_startup_seconds)" ||
  report_startup_failure "startup deadline expired before infrastructure validation."
if ! timeout --signal=TERM --kill-after=10s "${remaining}s" \
  bash scripts/validate-infrastructure.sh; then
  report_startup_failure "post-registration CDC readiness validation failed or exceeded the startup deadline."
fi
