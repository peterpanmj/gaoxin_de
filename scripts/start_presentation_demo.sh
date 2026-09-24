#!/usr/bin/env bash
# Start the stable local presentation environment from Git Bash.
# Requires a previously published Airflow release in the analytics-data volume.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd -P)"
COMPOSE_FILE="${PROJECT_ROOT}/infra/airflow/compose.yml"
ARTIFACTS_DIR="${PROJECT_ROOT}/artifacts"
RUNTIME_DIR="${PROJECT_ROOT}/var"
AIRFLOW_URL="http://localhost:8081"
DASH_URL="http://localhost:8051"

compose() {
  docker compose -f "${COMPOSE_FILE}" "$@"
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'Required command not found: %s\n' "$1" >&2
    exit 1
  fi
}

wait_for_url() {
  local url="$1"
  local name="$2"
  local attempt
  for attempt in $(seq 1 36); do
    if curl --noproxy '*' --fail --silent --show-error "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 5
  done
  printf '%s did not become ready: %s\n' "${name}" "${url}" >&2
  return 1
}

require_command docker
require_command uv
require_command curl

printf 'Starting Airflow...\n'
compose up -d

# Docker volumes survive a container recreation. Remove only a PID marker whose
# process no longer exists; this avoids the known Airflow standalone startup issue.
stale_pid="$(compose exec -T airflow sh -c '
  pid_file=/opt/airflow/airflow-webserver.pid
  if [ -f "$pid_file" ] && ! kill -0 "$(cat "$pid_file")" 2>/dev/null; then
    printf stale
  fi
' 2>/dev/null || true)"
if [ "${stale_pid}" = "stale" ]; then
  printf 'Removing stale Airflow webserver PID marker and restarting Airflow...\n'
  compose exec -T airflow rm -f /opt/airflow/airflow-webserver.pid
  compose up -d --force-recreate airflow
fi

printf 'Waiting for Airflow UI...\n'
wait_for_url "${AIRFLOW_URL}/health" "Airflow"

if ! release_id="$(compose exec -T airflow python -c '
import json
with open("/opt/data/warehouse/current.json", encoding="utf-8") as stream:
    print(json.load(stream)["release_id"])
' 2>/dev/null)"; then
  cat >&2 <<'MESSAGE'
No published release exists in the Airflow analytics-data volume.
Run a successful baseline DAG before using this presentation launcher.
MESSAGE
  exit 1
fi

export_root="${ARTIFACTS_DIR}/${release_id}"
if [ ! -f "${export_root}/warehouse/current.json" ]; then
  printf 'Exporting active release %s for Dash...\n' "${release_id}"
  compose exec -T airflow /opt/analytics/bin/saleor-analytics export-artifacts
fi

mkdir -p "${RUNTIME_DIR}"
if curl --noproxy '*' --fail --silent "${DASH_URL}/_dash-layout" >/dev/null 2>&1; then
  printf 'Dash is already listening at %s\n' "${DASH_URL}"
else
  printf 'Starting Dash from exported release %s...\n' "${release_id}"
  (
    export ANALYTICS_ROOT="${export_root}"
    nohup uv run saleor-analytics dashboard --host 127.0.0.1 --port 8051 \
      >"${RUNTIME_DIR}/presentation-dash.log" 2>&1 &
    printf '%s\n' "$!" >"${RUNTIME_DIR}/presentation-dash.pid"
  )
  wait_for_url "${DASH_URL}/_dash-layout" "Dash"
fi

printf '\nPresentation environment is ready.\n'
printf '  Airflow: %s\n' "${AIRFLOW_URL}"
printf '  Dash:    %s\n' "${DASH_URL}"
printf '  Release: %s\n' "${release_id}"
