#!/usr/bin/env bash
# One-command cold start (Reproducibility NFR): .env -> images -> stack up
# -> topics -> cluster ready. Idempotent: safe to re-run on a live stack.
# Usage: scripts/bootstrap.sh [--help]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

for arg in "$@"; do
  case "$arg" in
    -h|--help)
      echo "bootstrap.sh - cold start: env, images, stack, topics, readiness"
      echo "Run 'make nuke' first for a true cold-start rehearsal." ;;
    *) echo "unknown option $arg" >&2; exit 1 ;;
  esac
done

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker not found. Install Docker Desktop / Engine with Compose v2 first." >&2
  exit 1
fi

echo "[1/5] Preparing .env"
make --no-print-directory env

echo "[2/5] Building service images"
make --no-print-directory build-images

echo "[3/5] Starting stack"
make --no-print-directory up

echo "[4/5] Waiting for cluster"
bash scripts/wait-for-cluster.sh

echo "[5/5] Ready. Next: 'make smoke' verifies the end-to-end flow."
