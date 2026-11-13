#!/usr/bin/env bash
# 端到端演示: 先启动 docker compose up --build, 再运行本脚本
set -euo pipefail

A="${STATION_A:-http://localhost:8001}"
B="${STATION_B:-http://localhost:8002}"
P="${PROVINCE:-http://localhost:8000}"

if command -v jq >/dev/null 2>&1; then JQ="jq"; else JQ="cat"; fi
say() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }

say "0. 重置省库, 确认三服务在线"
curl -sS -X POST "$P/admin/reset" | $JQ
curl -sS "$A/health"; echo
curl -sS "$B/health"; echo

say "1. 录入修订: S1 两个修订(分别经 A/B 两个进程), S2/S3 各一个"
curl -sS -X POST "$A/revisions" -H 'Content-Type: application/json' -d '{"sample_no":"S1","concentration":25}' | $JQ
curl -sS -X POST "$B/revisions" -H 'Content-Type: application/json' -d '{"sample_no":"S1","concentration":30}' | $JQ
curl -sS -X POST "$B/revisions" -H 'Content-Type: application/json' -d '{"sample_no":"S2","concentration":41}' | $JQ
curl -sS -X POST "$A/revisions" -H 'Content-Type: application/json' -d '{"sample_no":"S3","concentration":7}'  | $JQ
sleep 4

say "2. 交接状态(应全部 acked, 注意 S1 的两个修订由两个进程协同按序发出)"
curl -sS "$A/revisions" | $JQ

say "3. 省库接收顺序(同一样本严格按修订号有序)"
curl -sS "$P/receipts" | $JQ

say "4. 模拟'收妥后丢回执': 省库已持久化, 但回执丢失"
curl -sS -X POST "$P/admin/config" -H 'Content-Type: application/json' -d '{"mode":"drop_receipt"}' | $JQ
curl -sS -X POST "$A/revisions" -H 'Content-Type: application/json' -d '{"sample_no":"S2","concentration":55}' | $JQ
sleep 3
echo "-- 该修订保持 pending 并持续重试(attempts 增长, last_error 可见):"
curl -sS "$A/samples/S2/revisions" | $JQ
echo "-- 省库其实已收到(按交接号去重, 只有一条):"
curl -sS "$P/receipts" | $JQ

say "5. 恢复回执: 重试拿到'原回执'(receipt.duplicate=true)"
curl -sS -X POST "$P/admin/config" -H 'Content-Type: application/json' -d '{"mode":"normal"}' | $JQ
sleep 4
curl -sS "$A/samples/S2/revisions" | $JQ

say "6. 慢响应不阻塞: 省库延迟 3000ms, 并行录入 3 个样本"
curl -sS -X POST "$P/admin/config" -H 'Content-Type: application/json' -d '{"delay_ms":3000}' | $JQ
t0=$(date +%s%3N)
for s in M1 M2 M3; do
  curl -sS -X POST "$A/revisions" -H 'Content-Type: application/json' -d "{\"sample_no\":\"$s\",\"concentration\":10}" >/dev/null &
done
wait
t1=$(date +%s%3N)
echo "本地录入 3 个样本耗时: $((t1-t0)) ms (不受省库慢响应影响)"
sleep 6
echo "-- 3 个样本并行发送, acked_at 应几乎相同(约 3 秒级, 而非串行的 9 秒级):"
curl -sS "$A/revisions?status=acked" | $JQ
curl -sS -X POST "$P/admin/config" -H 'Content-Type: application/json' -d '{"delay_ms":0}' >/dev/null

say "演示结束"
