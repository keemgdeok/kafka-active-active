#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
version="${1:-0.1.0}"
count="${2:-24}"
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z]+)*$ || ! "$count" =~ ^[1-9][0-9]{0,4}$ ]]; then
  echo "Usage: $0 [released-version] [healthy-event-count (1..99999)]" >&2
  exit 2
fi
scratch=$(mktemp -d)
project="kaa-release-$(basename "$scratch" | tr -cd '[:alnum:]' | tr '[:upper:]' '[:lower:]')"
export KAA_COMPOSE_FILE="$PWD/infra/compose.yaml"
compose=(docker compose --project-name "$project" -f "$KAA_COMPOSE_FILE")
mkdir -p build/reports/released-sdk
reports=$(mktemp -d "$PWD/build/reports/released-sdk/${version}-${count}.XXXXXX")
cleanup() {
  if [[ ! -f "$reports/events.jsonl" && -f "$scratch/app/events.jsonl" ]]; then
    cp "$scratch/app/events.jsonl" "$reports/events.jsonl" || true
  fi
  "${compose[@]}" logs --no-color >"$reports/brokers.log" 2>&1 || true
  "${compose[@]}" down --volumes >/dev/null 2>&1 || true
  rm -rf -- "$scratch"
  echo "Reports: $reports"
}
trap cleanup EXIT
cp -R examples/released-sdk "$scratch/app"
{
  uname -smr
  java -version
  docker info --format 'Docker server {{.ServerVersion}} / {{.Architecture}}, {{.NCPU}} CPUs, {{.MemTotal}} bytes'
} >"$reports/environment.txt" 2>&1
"${compose[@]}" up -d --wait --wait-timeout 90 kafka-a kafka-b
./gradlew --no-daemon --gradle-user-home "$scratch/gradle-home" --project-dir "$scratch/app" \
  --console=plain --info -PsdkVersion="$version" run --args="$version $count $project" >"$reports/app.log" 2>&1 || {
  tail -70 "$reports/app.log" >&2
  exit 1
}
cp "$scratch/app/events.jsonl" "$reports/events.jsonl"
grep -F "Downloading https://repo.maven.apache.org/maven2/io/github/keemgdeok/kafka-active-active/$version/kafka-active-active-$version.jar" "$reports/app.log"
cat "$reports/events.jsonl"
