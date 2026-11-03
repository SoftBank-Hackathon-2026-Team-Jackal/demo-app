#!/bin/bash
# 사용법: scripts/weight.sh <app-a 가중치> <app-b 가중치>
#   예) scripts/weight.sh 50 50   → 트래픽 반반
#       scripts/weight.sh 0 100   → 전부 app-b
set -euo pipefail
a="${1:?app-a weight}"; b="${2:?app-b weight}"
send() { printf '%s\n' "$1" | nc -w 1 127.0.0.1 "${HAPROXY_RUNTIME_PORT:-9999}"; }
send "set server app/app-a weight $a"
send "set server app/app-b weight $b"
send "show servers state app" | awk '$4 ~ /^app-/ {print $4, "weight=" $8}'
