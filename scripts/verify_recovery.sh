#!/usr/bin/env bash
# 不依赖 Docker 的本地验证: 起 1 个省库 + 2 个站内进程(共用空白卷), 跑恢复集成测试
set -euo pipefail
cd "$(dirname "$0")/.."

python3 -m pip show fastapi >/dev/null 2>&1 || {
  echo "缺少依赖, 先执行: python3 -m pip install -r station/requirements.txt"; exit 1; }
python3 -m pip show python-multipart >/dev/null 2>&1 || {
  echo "缺少 python-multipart (上传备份必需)"; exit 1; }

python3 scripts/make_sample_backup.py
python3 tests/test_recovery_e2e.py
python3 tests/test_recovery_edge.py
echo "本地恢复验证全部通过"
