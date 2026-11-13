#!/usr/bin/env python3
"""把样例省库记录写入省库, 使演示场景可复现(需在省库未运行或可写时执行)。

默认读取 samples/province_receipts.json, 写入 PROVINCE_DB 指向的 SQLite。
也可在省库进程运行时用 seed_province.py --via-api 通过 /handoff 灌入。
"""
import json
import os
import sqlite3
import sys
import time
import urllib.request


def seed_db(db_path: str, receipts: list[dict]) -> None:
    conn = sqlite3.connect(db_path, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS receipts (
            seq          INTEGER PRIMARY KEY AUTOINCREMENT,
            handoff_id   TEXT NOT NULL UNIQUE,
            receipt_id   TEXT NOT NULL,
            station_code TEXT,
            sample_no    TEXT NOT NULL,
            revision_no  INTEGER NOT NULL,
            concentration INTEGER NOT NULL,
            unit         TEXT NOT NULL DEFAULT 'ug/L',
            payload_json TEXT NOT NULL,
            received_at  REAL NOT NULL
        )"""
    )
    now = time.time()
    for r in receipts:
        try:
            cur = conn.execute(
                "INSERT INTO receipts (handoff_id, receipt_id, station_code, sample_no,"
                " revision_no, concentration, unit, payload_json, received_at)"
                " VALUES (?, '', ?, ?, ?, ?, ?, ?, ?)",
                (r["handoff_id"], r.get("station_code", "ST01"), r["sample_no"],
                 int(r["revision_no"]), int(r["concentration"]),
                 r.get("unit", "ug/L"), json.dumps(r, ensure_ascii=False), now),
            )
        except sqlite3.IntegrityError:
            continue  # 已存在, 保留原记录
        seq = cur.lastrowid
        conn.execute("UPDATE receipts SET receipt_id = ? WHERE seq = ?",
                     (f"RCPT-{seq:06d}", seq))
    conn.commit()
    n = conn.execute("SELECT COUNT(*) FROM receipts").fetchone()[0]
    conn.close()
    print(f"province seeded: {n} receipt(s) in {db_path}")


def seed_api(base: str, receipts: list[dict]) -> None:
    for r in receipts:
        body = json.dumps({**r, "attempt": 1}).encode()
        req = urllib.request.Request(
            base.rstrip("/") + "/handoff", data=body,
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
    print(f"province seeded via API: {len(receipts)} handoff(s) at {base}")


def main() -> None:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(root, "samples", "province_receipts.json")
    with open(path, encoding="utf-8") as f:
        receipts = json.load(f)
    if "--via-api" in sys.argv:
        seed_api(os.environ.get("PROVINCE", "http://localhost:8000"), receipts)
    else:
        db = os.environ.get("PROVINCE_DB", os.path.join(root, "province", "province.db"))
        seed_db(db, receipts)


if __name__ == "__main__":
    main()
