#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="vehicle-cache"
KUBE_CONTEXT="${KUBE_CONTEXT:-docker-desktop}"

if ! kubectl config get-contexts -o name | grep -Fxq "$KUBE_CONTEXT"; then
  echo "Kubernetes context '${KUBE_CONTEXT}' is not configured."
  exit 0
fi

if ! kubectl --context "$KUBE_CONTEXT" get namespace "$NAMESPACE" >/dev/null 2>&1; then
  echo "Project namespace '${NAMESPACE}' is not deployed."
  exit 0
fi

kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
  get statefulsets,deployments,jobs,pods,services,persistentvolumeclaims

connector_status="$(kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" run connector-health \
  --image=curlimages/curl:8.12.1 \
  --restart=Never \
  --rm -i \
  --pod-running-timeout=60s \
  --command -- sh -ec \
  'curl --fail --silent --show-error http://kafka-connect:8083/connectors/vehicle-cdc-connector/status')"
running_count="$(printf '%s' "$connector_status" |
  grep -o '"state"[[:space:]]*:[[:space:]]*"RUNNING"' |
  wc -l || true)"
if ((running_count < 2)); then
  echo "Debezium connector or task is not RUNNING: $connector_status" >&2
  exit 1
fi
printf 'Debezium connector and task are RUNNING: %s\n' "$connector_status"

kubectl --context "$KUBE_CONTEXT" get namespace "$NAMESPACE" \
  -o 'jsonpath=Snapshot readiness: {.metadata.annotations.vehicle-cache\.dev/snapshot-ready}{"\n"}'
