#!/usr/bin/env python3
"""
生成恢复演示用样例数据:
- samples/old_backup.db      旧站内备份(含已确认/未确认修订, 缺少部分省库已收记录)
- samples/conflict_backup.db 冲突备份(与省库样例存在同修订号串号/浓度矛盾)
- samples/province_receipts.json 省库样例(用于 seed_province.py)
- samples/expected_after_recovery.json 恢复后应看到的状态(便于核对)
"""
import json
import os
import sqlite3
import time
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
SAMPLES = os.path.join(os.path.dirname(HERE), "samples")
STATION_CODE = "ST01"

SCHEMA = """
CREATE TABLE IF NOT EXISTS revisions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_no     TEXT NOT NULL,
    revision_no   INTEGER NOT NULL,
    concentration INTEGER NOT NULL CHECK (concentration >= 0),
    unit          TEXT NOT NULL DEFAULT 'ug/L',
    handoff_id    TEXT NOT NULL UNIQUE,
    status        TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'acked')),
    attempts      INTEGER NOT NULL DEFAULT 0,
    claimed_by    TEXT,
    claim_until   REAL,
    last_error    TEXT,
    receipt_json  TEXT,
    created_at    REAL NOT NULL,
    last_sent_at  REAL,
    acked_at      REAL,
    UNIQUE (sample_no, revision_no)
);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""


def ts(s: str) -> float:
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp()


T0 = ts("2026-10-01T08:00:00")
OLD_RECEIPT = {
    "receipt_id": "RCPT-OLD-000001", "status": "ACCEPTED", "duplicate": False,
    "received_seq": 1, "received_at": "2026-10-01T08:00:10+00:00",
}

# 备份中的修订: (样本, 修订号, 浓度, 交接号, 状态, attempts, acked_at, 回执)
BACKUP_ROWS = [
    ("W2026-0001", 1, 25, "ST01-W2026-0001-r1-a1b2c3", "acked", 2, T0 + 10,
     {"receipt_id": "RCPT-OLD-000001", "handoff_id": "ST01-W2026-0001-r1-a1b2c3",
      "status": "ACCEPTED", "duplicate": False, "received_seq": 1,
      "received_at": "2026-10-01T08:00:10+00:00"}),
    ("W2026-0001", 2, 30, "ST01-W2026-0001-r2-d4e5f6", "pending", 3, None, None),
    # 省库从未收到 W2026-0002, 恢复后按原交接号续交
    ("W2026-0002", 1, 41, "ST01-W2026-0002-r1-112233", "pending", 1, None, None),
    ("W2026-0004", 1, 12, "ST01-W2026-0004-r1-445566", "acked", 1, T0 + 40,
     {"receipt_id": "RCPT-OLD-000004", "handoff_id": "ST01-W2026-0004-r1-445566",
      "status": "ACCEPTED", "duplicate": False, "received_seq": 4,
      "received_at": "2026-10-01T08:00:40+00:00"}),
    ("W2026-0004", 2, 13, "ST01-W2026-0004-r2-778899", "pending", 2, None, None),
    ("W2026-0005", 1, 18, "ST01-W2026-0005-r1-aabbcc", "acked", 1, T0 + 50,
     {"receipt_id": "RCPT-OLD-000005", "handoff_id": "ST01-W2026-0005-r1-aabbcc",
      "status": "ACCEPTED", "duplicate": False, "received_seq": 5,
      "received_at": "2026-10-01T08:00:50+00:00"}),
    ("W2026-0005", 2, 19, "ST01-W2026-0005-r2-ddeeff", "acked", 1, T0 + 60,
     {"receipt_id": "RCPT-OLD-000006", "handoff_id": "ST01-W2026-0005-r2-ddeeff",
      "status": "ACCEPTED", "duplicate": False, "received_seq": 6,
      "received_at": "2026-10-01T08:01:00+00:00"}),
]

# 省库样例: 备份缺少 W2026-0001 r3、W2026-0003 整样本; W2026-0001 r2 已收但回执丢失
PROVINCE_ROWS = [
    ("ST01-W2026-0001-r1-a1b2c3", "W2026-0001", 1, 25),
    ("ST01-W2026-0001-r2-d4e5f6", "W2026-0001", 2, 30),
    ("ST01-W2026-0001-r3-abcdef", "W2026-0001", 3, 28),
    ("ST01-W2026-0003-r1-135790", "W2026-0003", 1, 50),
    ("ST01-W2026-0003-r2-246801", "W2026-0003", 2, 55),
    ("ST01-W2026-0004-r1-445566", "W2026-0004", 1, 12),
    ("ST01-W2026-0005-r1-aabbcc", "W2026-0005", 1, 18),
    ("ST01-W2026-0005-r2-ddeeff", "W2026-0005", 2, 19),
]

# 冲突备份: 与省库样例的同一样本同一修订号交接号/浓度不一致
CONFLICT_ROWS = [
    ("W2026-0001", 1, 25, "ST01-W2026-0001-r1-WRONG0", "acked", 1, T0 + 10,
     {"receipt_id": "RCPT-OLD-000001", "handoff_id": "ST01-W2026-0001-r1-WRONG0",
      "status": "ACCEPTED", "duplicate": False, "received_seq": 1,
      "received_at": "2026-10-01T08:00:10+00:00"}),
    ("W2026-0001", 2, 30, "ST01-W2026-0001-r2-d4e5f6", "pending", 3, None, None),
    ("W2026-0003", 1, 51, "ST01-W2026-0003-r1-135790", "acked", 1, T0 + 40,
     {"receipt_id": "RCPT-OLD-000009", "handoff_id": "ST01-W2026-0003-r1-135790",
      "status": "ACCEPTED", "duplicate": False, "received_seq": 9,
      "received_at": "2026-10-01T08:02:00+00:00"}),
]


def build(path: str, rows: list) -> None:
    if os.path.exists(path):
        os.remove(path)
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.execute("INSERT INTO meta (key, value) VALUES ('station_code', ?)", (STATION_CODE,))
    for i, (sample, rev, conc, handoff, status, attempts, acked_at, receipt) in enumerate(rows):
        conn.execute(
            "INSERT INTO revisions (sample_no, revision_no, concentration, handoff_id,"
            " status, attempts, receipt_json, created_at, last_sent_at, acked_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                sample, rev, conc, handoff, status, attempts,
                json.dumps(receipt, ensure_ascii=False) if receipt else None,
                T0 + i * 20,
                (T0 + i * 20 + 5) if status == "acked" else None,
                acked_at,
            ),
        )
    conn.commit()
    conn.close()


def main() -> None:
    os.makedirs(SAMPLES, exist_ok=True)
    build(os.path.join(SAMPLES, "old_backup.db"), BACKUP_ROWS)
    build(os.path.join(SAMPLES, "conflict_backup.db"), CONFLICT_ROWS)
    with open(os.path.join(SAMPLES, "province_receipts.json"), "w", encoding="utf-8") as f:
        json.dump(
            [
                {"handoff_id": h, "station_code": STATION_CODE, "sample_no": s,
                 "revision_no": r, "concentration": c, "unit": "ug/L"}
                for h, s, r, c in PROVINCE_ROWS
            ],
            f, ensure_ascii=False, indent=2,
        )
    print("generated samples/old_backup.db, samples/conflict_backup.db,"
          " samples/province_receipts.json")


if __name__ == "__main__":
    main()
