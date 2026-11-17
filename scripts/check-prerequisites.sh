#!/usr/bin/env bash
set -euo pipefail

KUBE_CONTEXT="${KUBE_CONTEXT:-docker-desktop}"

missing=()
for command_name in docker kubectl devspace openssl timeout; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    missing+=("$command_name")
  fi
done

if ((${#missing[@]} > 0)); then
  printf 'Missing required commands: %s\n' "${missing[*]}" >&2
  printf 'Run this workflow from the configured WSL2 devenv shell.\n' >&2
  exit 1
fi

if ! timeout --signal=TERM --kill-after=5s 30s docker version --format '{{.Server.Version}}' >/dev/null 2>&1; then
  echo "Docker Engine is unavailable or did not respond within 30 seconds." >&2
  echo "Start Docker and enable Docker Desktop WSL integration before running just up." >&2
  exit 1
fi

if ! kubectl config get-contexts -o name | grep -Fxq "$KUBE_CONTEXT"; then
  echo "Kubernetes context '${KUBE_CONTEXT}' is not configured." >&2
  exit 1
fi

if ! timeout --signal=TERM --kill-after=5s 30s \
  kubectl --context "$KUBE_CONTEXT" get --raw=/readyz >/dev/null 2>&1; then
  printf "Kubernetes API for context '%s' is unavailable or did not respond within 30 seconds.\n" \
    "$KUBE_CONTEXT" >&2
  exit 1
fi
