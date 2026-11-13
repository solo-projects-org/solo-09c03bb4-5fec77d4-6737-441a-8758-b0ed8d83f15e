"""
省级监测数仓接收端(本地模拟)

- 按交接号(handoff_id)持久去重: 重复报送返回首次生成的原回执(duplicate=true)
- 可模拟"收妥后丢回执": 数据已持久化, 但客户端收到 500(回执丢失), 用于验证站内重试 + 省库去重
- 可模拟慢响应(delay_ms), 用于验证站内并行发送不互相阻塞
- GET /receipts 查看省库实际接收顺序
"""

import asyncio
import json
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Literal

from fastapi import FastAPI
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, Field

DB_PATH = os.environ.get("PROVINCE_DB", "province.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS receipts (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,  -- 接收顺序号
    handoff_id   TEXT NOT NULL UNIQUE,               -- 持久去重键
    receipt_id   TEXT NOT NULL,
    station_code TEXT,
    sample_no    TEXT NOT NULL,
    revision_no  INTEGER NOT NULL,
    concentration INTEGER NOT NULL,
    unit         TEXT NOT NULL DEFAULT 'ug/L',
    payload_json TEXT NOT NULL,
    received_at  REAL NOT NULL
);
"""

# 运行期可调的故障注入配置(仅内存, 不影响 receipts 的持久性)
CONFIG = {
    "mode": os.environ.get("PROVINCE_MODE", "normal"),  # normal | drop_receipt
    "delay_ms": int(os.environ.get("PROVINCE_DELAY_MS", "0")),  # 每次请求的处理延迟
    "drop_next": 0,  # 接下来 N 次"收妥后丢回执"(一次性)
}


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=15000")
    return conn


def init_db() -> None:
    d = os.path.dirname(DB_PATH)
    if d:
        os.makedirs(d, exist_ok=True)
    conn = _connect()
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()


def _query(sql: str, params: tuple = ()) -> list[dict]:
    conn = _connect()
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def _execute(sql: str, params: tuple = ()) -> int:
    conn = _connect()
    try:
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def _record(p: dict) -> tuple[dict, bool]:
    """按交接号持久去重: 新交接号落库并生成回执; 重复交接号返回原回执。"""
    now = time.time()
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "INSERT INTO receipts (handoff_id, receipt_id, station_code, sample_no,"
                " revision_no, concentration, unit, payload_json, received_at)"
                " VALUES (?, '', ?, ?, ?, ?, ?, ?, ?)",
                (
                    p["handoff_id"],
                    p.get("station_code"),
                    p["sample_no"],
                    p["revision_no"],
                    p["concentration"],
                    p.get("unit") or "ug/L",
                    json.dumps(p, ensure_ascii=False),
                    now,
                ),
            )
        except sqlite3.IntegrityError:  # handoff_id 已存在 → 重复报送
            row = conn.execute(
                "SELECT * FROM receipts WHERE handoff_id = ?", (p["handoff_id"],)
            ).fetchone()
            conn.commit()
            return dict(row), True
        seq = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        receipt_id = f"RCPT-{seq:06d}"
        conn.execute("UPDATE receipts SET receipt_id = ? WHERE seq = ?", (receipt_id, seq))
        row = conn.execute("SELECT * FROM receipts WHERE seq = ?", (seq,)).fetchone()
        conn.commit()
        return dict(row), False
    finally:
        conn.close()


def _iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _receipt_response(row: dict, duplicate: bool) -> dict:
    return {
        "receipt_id": row["receipt_id"],
        "handoff_id": row["handoff_id"],
        "status": "ACCEPTED",
        "duplicate": duplicate,
        "received_seq": row["seq"],
        "received_at": _iso(row["received_at"]),
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="省级监测数仓(本地模拟)",
    version="1.0.0",
    description=(
        "按交接号持久去重, 重复报送返回原回执。\n\n"
        "故障注入(POST /admin/config):\n"
        "- `mode=drop_receipt`: 收妥后丢回执(已持久化, 客户端收 500)\n"
        "- `delay_ms`: 慢响应\n"
        "- `drop_next`: 接下来 N 次丢回执"
    ),
    lifespan=lifespan,
)


class HandoffIn(BaseModel):
    handoff_id: str = Field(min_length=1, max_length=128)
    station_code: str | None = None
    sample_no: str = Field(min_length=1, max_length=64)
    revision_no: int = Field(ge=1)
    concentration: int = Field(ge=0)
    unit: str = "ug/L"
    attempt: int | None = None
    sent_at: str | None = None


class ConfigIn(BaseModel):
    mode: Literal["normal", "drop_receipt"] | None = None
    delay_ms: int | None = Field(None, ge=0, le=120000)
    drop_next: int | None = Field(None, ge=0, le=10000)


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse("/docs")


@app.post("/handoff", tags=["handoff"], summary="接收站内交接(按交接号去重)")
async def handoff(body: HandoffIn):
    if CONFIG["delay_ms"] > 0:
        await asyncio.sleep(CONFIG["delay_ms"] / 1000)

    row, duplicate = await asyncio.to_thread(_record, body.model_dump())

    drop = False
    if CONFIG["drop_next"] > 0:
        CONFIG["drop_next"] -= 1
        drop = True
    elif CONFIG["mode"] == "drop_receipt":
        drop = True
    if drop:
        # 收妥后丢回执: 数据已持久化, 但回执未送达客户端
        return JSONResponse(
            status_code=500,
            content={"detail": "receipt lost in transit (simulated); handoff was persisted"},
        )
    return _receipt_response(row, duplicate)


@app.get("/receipts", tags=["receipts"], summary="查看省库接收顺序")
async def list_receipts():
    rows = await asyncio.to_thread(_query, "SELECT * FROM receipts ORDER BY seq")
    return [
        {
            "received_seq": r["seq"],
            "receipt_id": r["receipt_id"],
            "handoff_id": r["handoff_id"],
            "station_code": r["station_code"],
            "sample_no": r["sample_no"],
            "revision_no": r["revision_no"],
            "concentration": r["concentration"],
            "unit": r["unit"],
            "received_at": _iso(r["received_at"]),
        }
        for r in rows
    ]


@app.get("/admin/config", tags=["admin"], summary="查看故障注入配置")
async def get_config():
    return CONFIG


@app.post("/admin/config", tags=["admin"], summary="设置故障注入(部分字段更新)")
async def set_config(body: ConfigIn):
    for k, v in body.model_dump(exclude_unset=True).items():
        if v is not None:
            CONFIG[k] = v
    return CONFIG


@app.post("/admin/reset", tags=["admin"], summary="清空接收记录(演示用)")
async def reset():
    await asyncio.to_thread(_execute, "DELETE FROM receipts")
    return {"ok": True}


@app.get("/health", include_in_schema=False)
async def health():
    return {"ok": True}
