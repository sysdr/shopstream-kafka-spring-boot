#!/usr/bin/env bash
# Runs every lesson end to end: start.sh (build, tests, Kafka, services, verify, demo), verify.sh again, stop.sh.
# Takes about 15 minutes (Day 3's rebalance storm and Day 4's three broker kills are deliberately slow).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
PASS=0
for n in 1 2 3 4 5 6; do
  d="$ROOT/lessons/lesson-$n"
  echo "=== Lesson $n ==="
  (cd "$d" && bash start.sh > "/tmp/shopstream-kafka-lesson-$n.log" 2>&1 && bash verify.sh | tail -1 && bash stop.sh >/dev/null) \
    || { echo "FAIL lesson $n (log: /tmp/shopstream-kafka-lesson-$n.log)"; (cd "$d" && bash stop.sh >/dev/null 2>&1 || true); exit 1; }
  PASS=$((PASS+1))
done
echo "All $PASS lessons passed."
