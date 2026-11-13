#!/usr/bin/env bash
# 恢复演示: 站内数据卷损坏后, 在空白卷上以恢复模式重建交接进度
#
#   bash scripts/demo_recovery.sh
#
# 前置: docker compose 可用; 会清空本项目的 province-data / station-data 卷。
set -euo pipefail
cd "$(dirname "$0")/.."

A="${STATION_A:-http://localhost:8001}"
B="${STATION_B:-http://localhost:8002}"
P="${PROVINCE:-http://localhost:8000}"
SAMPLES="$(pwd)/samples"

if command -v jq >/dev/null 2>&1; then JQ="jq"; else JQ="cat"; fi
say() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
code() { curl -sS -o /tmp/tp_body.json -w '%{http_code}' "$@"; }

say "0. 清空卷, 启动省库并灌入省库样例"
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d province
until curl -sf "$P/health" >/dev/null; do sleep 1; done
python3 scripts/seed_province.py "$SAMPLES/province_receipts.json" --via-api
echo "-- 省库已收记录(恢复期间不再有写入):"
curl -sS "$P/receipts" | $JQ

say "1. 空白站内数据卷, 以恢复模式启动两个站内进程"
docker compose --profile recovery up -d --build station-a-recovery station-b-recovery
for u in "$A/health" "$B/health"; do
  until curl -s "$u" | grep -q '"ok":true'; do sleep 1; done
done
curl -sS "$A/recovery/status" | $JQ

say "2. 恢复完成前两个进程都封闭录入与查询(HTTP 423)"
echo -n "station-a POST /revisions -> "; code -X POST "$A/revisions" \
  -H 'Content-Type: application/json' -d '{"sample_no":"W2026-0009","concentration":8}'; echo
echo -n "station-b GET  /revisions -> "; code "$B/revisions"; echo

say "3. 先上传冲突备份: 同一样本同一修订号交接号/浓度不一致 -> 409, 不覆盖任一方"
echo -n "POST /recovery (conflict_backup.db) -> "
code -X POST "$A/recovery" -F "backup=@$SAMPLES/conflict_backup.db"; echo
cat /tmp/tp_body.json | $JQ
echo "-- 冲突后仍封闭:"
curl -sS "$B/recovery/status" | $JQ

say "4. 改用旧备份恢复: 备份 + 省库合并, 单个事务生效"
echo -n "POST /recovery (old_backup.db) -> "
code -X POST "$A/recovery" -F "backup=@$SAMPLES/old_backup.db"; echo
cat /tmp/tp_body.json | $JQ

say "5. 两个进程同时开放(省库已收标为已确认, 其余续交)"
curl -sS "$A/recovery/status" | $JQ
curl -sS "$B/recovery/status" | $JQ
sleep 4

say "6. Swagger/API 回查恢复结果"
echo "-- station-a: 全部修订"
curl -sS "$A/revisions" | $JQ
echo "-- 从省库补回的 W2026-0003(备份中没有)"
curl -sS "$A/samples/W2026-0003/revisions" | $JQ
echo "-- 省库接收顺序(备份中 pending 的修订已按原交接号续交)"
curl -sS "$P/receipts" | $JQ

say "7. 恢复后继续交接: 新修订号从历史最大号接续"
curl -sS -X POST "$A/revisions" -H 'Content-Type: application/json' \
  -d '{"sample_no":"W2026-0001","concentration":99}' | $JQ
curl -sS -X POST "$B/revisions" -H 'Content-Type: application/json' \
  -d '{"sample_no":"W2026-0003","concentration":70}' | $JQ
sleep 4
echo "-- 新修订应为 W2026-0001 r4、W2026-0003 r3 且已 acked:"
curl -sS "$A/revisions?sample_no=W2026-0001" | $JQ
curl -sS "$A/revisions?sample_no=W2026-0003" | $JQ

say "8. 切回日常(非恢复)模式"
docker compose --profile recovery stop station-a-recovery station-b-recovery
docker compose up -d station-a station-b
for u in "$A/health" "$B/health"; do
  until curl -s "$u" | grep -q '"ok":true'; do sleep 1; done
done
curl -sS "$A/status" | $JQ

say "演示结束: Swagger 见 $A/docs 与 $B/docs"
