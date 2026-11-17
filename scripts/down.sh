#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="vehicle-cache"
KUBE_CONTEXT="${KUBE_CONTEXT:-docker-desktop}"

if ! kubectl config get-contexts -o name | grep -Fxq "$KUBE_CONTEXT"; then
  echo "Kubernetes context '${KUBE_CONTEXT}' is not configured; nothing to remove."
  exit 0
fi

kubectl --context "$KUBE_CONTEXT" delete namespace "$NAMESPACE" \
  --ignore-not-found=true --wait=true --timeout=180s
echo "Removed project namespace '${NAMESPACE}' from context '${KUBE_CONTEXT}'."
