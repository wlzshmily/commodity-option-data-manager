#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

HOST="${ODM_WEB_HOST:-127.0.0.1}"
PORT="${ODM_WEB_PORT:-8765}"
BASE_URL="http://${HOST}:${PORT}"
DATABASE_PATH="${ODM_DATABASE_PATH:-data/option-data-current.sqlite3}"
RUNTIME_DIR="${ODM_RUNTIME_DIR:-data/runtime}"
LOG_DIR="${ODM_LOG_DIR:-data}"
PID_FILE="${ODM_WEBUI_PID_FILE:-${RUNTIME_DIR}/odm-webui.pid}"
OUT_LOG="${ODM_WEBUI_OUT_LOG:-${LOG_DIR}/webui-${PORT}.out.log}"
ERR_LOG="${ODM_WEBUI_ERR_LOG:-${LOG_DIR}/webui-${PORT}.err.log}"
REPORT_DIR="${ODM_QUOTE_STREAM_REPORT_DIR:-docs/qa/live-evidence/quote-stream-runtime}"

mkdir -p "$RUNTIME_DIR" "$LOG_DIR" "$REPORT_DIR"

usage() {
  cat <<EOF
Usage: $(basename "$0") {start|stop|restart|status}

Environment overrides:
  ODM_WEB_HOST        default: ${HOST}
  ODM_WEB_PORT        default: ${PORT}
  ODM_DATABASE_PATH   default: ${DATABASE_PATH}
  ODM_RUNTIME_DIR     default: ${RUNTIME_DIR}
EOF
}

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

health_ok() {
  curl -fsS --connect-timeout 1 --max-time 3 "${BASE_URL}/api/health" >/dev/null 2>&1
}

post_best_effort() {
  local path="$1"
  curl -fsS -X POST --connect-timeout 1 --max-time 8 "${BASE_URL}${path}" \
    >/dev/null 2>&1 || true
}

process_pids() {
  local pattern="$1"
  ps -eo pid=,args= \
    | awk -v pat="$pattern" '$0 ~ pat && $0 !~ /awk -v pat/ {print $1}'
}

terminate_pids() {
  local label="$1"
  shift
  local pids=("$@")
  if [ "${#pids[@]}" -eq 0 ]; then
    return 0
  fi
  log "Stopping ${label}: ${pids[*]}"
  kill "${pids[@]}" 2>/dev/null || true
  sleep 2
  local alive=()
  local pid
  for pid in "${pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      alive+=("$pid")
    fi
  done
  if [ "${#alive[@]}" -gt 0 ]; then
    log "Force stopping ${label}: ${alive[*]}"
    kill -9 "${alive[@]}" 2>/dev/null || true
  fi
}

stop_workers() {
  log "Requesting realtime workers to stop."
  post_best_effort "/api/quote-stream/stop"
  post_best_effort "/api/contract-manager/stop"
  touch "${REPORT_DIR}"/worker-*.stop 2>/dev/null || true
  sleep 2
  mapfile -t quote_pids < <(process_pids 'option_data_manager[.]cli[.]quote_stream')
  mapfile -t metrics_pids < <(process_pids 'option_data_manager[.]cli[.]metrics_worker')
  terminate_pids "quote-stream workers" "${quote_pids[@]}"
  terminate_pids "metrics workers" "${metrics_pids[@]}"
}

stop_webui() {
  local pids=()
  if [ -f "$PID_FILE" ]; then
    local recorded_pid
    recorded_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ "$recorded_pid" =~ ^[0-9]+$ ]] && kill -0 "$recorded_pid" 2>/dev/null; then
      pids+=("$recorded_pid")
    fi
  fi
  mapfile -t discovered < <(process_pids '(^| )uv run odm-webui( |$)|/odm-webui( |$)')
  pids+=("${discovered[@]}")
  if [ "${#pids[@]}" -gt 0 ]; then
    mapfile -t pids < <(printf '%s\n' "${pids[@]}" | awk 'NF && !seen[$1]++')
  fi
  terminate_pids "WebUI server" "${pids[@]}"
  rm -f "$PID_FILE"
}

print_lock_holders() {
  if command -v fuser >/dev/null 2>&1 && [ -f "$DATABASE_PATH" ]; then
    local holders
    holders="$(fuser "$DATABASE_PATH" 2>/dev/null || true)"
    if [ -n "$holders" ]; then
      log "SQLite file is open by PID(s): ${holders}"
      ps -fp $holders 2>/dev/null || true
    fi
  fi
}

start_server() {
  if health_ok; then
    log "WebUI already responds at ${BASE_URL}"
    return 0
  fi
  stop_workers
  print_lock_holders
  log "Starting WebUI at ${BASE_URL}"
  : >"$OUT_LOG"
  : >"$ERR_LOG"
  (
    export ODM_WEB_HOST="$HOST"
    export ODM_WEB_PORT="$PORT"
    export ODM_DATABASE_PATH="$DATABASE_PATH"
    exec setsid uv run odm-webui >>"$OUT_LOG" 2>>"$ERR_LOG"
  ) &
  local pid="$!"
  printf '%s\n' "$pid" >"$PID_FILE"
  for _ in {1..30}; do
    if health_ok; then
      log "WebUI started: ${BASE_URL}"
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      log "WebUI process exited during startup. stderr:"
      tail -n 80 "$ERR_LOG" || true
      return 1
    fi
    sleep 1
  done
  log "WebUI did not become healthy within 30 seconds. stderr:"
  tail -n 80 "$ERR_LOG" || true
  return 1
}

stop_server() {
  stop_workers
  stop_webui
  print_lock_holders
  if health_ok; then
    log "Warning: WebUI still responds at ${BASE_URL}"
    return 1
  fi
  log "Server stopped."
}

status_server() {
  if health_ok; then
    log "WebUI healthy: ${BASE_URL}"
  else
    log "WebUI not responding: ${BASE_URL}"
  fi
  ps -eo pid,ppid,etime,cmd \
    | grep -E 'odm-webui|option_data_manager[.]cli[.](quote_stream|metrics_worker)' \
    | grep -v grep || true
  print_lock_holders
}

case "${1:-}" in
  start)
    start_server
    ;;
  stop)
    stop_server
    ;;
  restart)
    stop_server || true
    start_server
    ;;
  status)
    status_server
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage
    exit 2
    ;;
esac
