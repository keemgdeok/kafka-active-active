#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
demo_project="kaa-demo-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:8])')"
mkdir -p build/reports/demo
cleanup() {
  docker compose --project-name "$demo_project" -f infra/compose.yaml logs --no-color >build/reports/demo/brokers.log 2>&1 || true
  docker compose --project-name "$demo_project" -f infra/compose.yaml down --volumes >/dev/null 2>&1 || true
}
trap cleanup EXIT
docker compose --project-name "$demo_project" -f infra/compose.yaml up -d --wait kafka-a kafka-b
./gradlew --no-daemon run --args="--compose-project $demo_project --count ${1:-20} --seed ${2:-0}"
