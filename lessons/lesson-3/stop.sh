#!/usr/bin/env bash
# Stop the services and the Kafka brokers started by start.sh (exits 0 even if nothing runs).
ROOT="$(cd "$(dirname "$0")" && pwd)"
RUN="$ROOT/shopstream-events/.run"
[ -d "$RUN" ] || { echo "[INFO] Nothing to stop."; exit 0; }
stop_pid() {
  local f="$1" pid
  pid="$(cat "$f" 2>/dev/null || true)"
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 50); do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
    kill -9 "$pid" 2>/dev/null || true
    echo "[INFO] stopped $(basename "$f" .pid)"
  fi
  rm -f "$f"
}
for f in "$RUN"/*.pid; do
  case "$f" in *kafka-*) ;; *) [ -f "$f" ] && stop_pid "$f" ;; esac
done
for f in "$RUN"/kafka-*.pid; do [ -f "$f" ] && stop_pid "$f"; done
exit 0
