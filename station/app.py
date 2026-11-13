"""
站内交接服务 (station)

- LIMS 审核后的总磷结果录入为"修订"(revision): 样本号 + 非负整数浓度(μg/L)
- 每个修订生成样本内递增的修订号(revision_no)和固定交接号(handoff_id)
- 只有成功落库(事务提交)的修订才会被后台发送器投递到省库
- 未获收妥确认的修订持续重试(指数退避); 进程重启后自动续交
- 同一样本严格按修订顺序发送(前一份未确认, 下一份不发送); 不同样本并行
- 两个服务进程共用同一 SQLite 库, 通过租约(claim)协调占用, 不会重复发送同一修订;
  即便极端情况下重复发送, 省库按交接号去重也会返回原回执, 因此端到端安全
"""

import asyncio
import json
import os
import secrets
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

# ---------------------------------------------------------------- 配置

DB_PATH = os.environ.get("STATION_DB", "station.db")
PROVINCE_URL = os.environ.get("PROVINCE_URL", "http://localhost:8000").rstrip("/")
INSTANCE_ID = os.environ.get("INSTANCE_ID", f"station-{os.getpid()}")
STATION_CODE = os.environ.get("STATION_CODE", "ST01")
SEND_TIMEOUT_S = float(os.environ.get("SEND_TIMEOUT_S", "5"))      # 单次发送超时
POLL_INTERVAL_S = float(os.environ.get("POLL_INTERVAL_S", "1.0"))  # 发送器轮询间隔
CLAIM_LEASE_S = float(os.environ.get("CLAIM_LEASE_S", "15"))       # 认领租约时长(进程崩溃后租约过期, 由其他进程接管)
MAX_INFLIGHT = int(os.environ.get("MAX_INFLIGHT", "50"))           # 单进程并发发送上限

SCHEMA = """
CREATE TABLE IF NOT EXISTS revisions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_no     TEXT NOT NULL,
    revision_no   INTEGER NOT NULL,
    concentration INTEGER NOT NULL CHECK (concentration >= 0),
    unit          TEXT NOT NULL DEFAULT 'ug/L',
    handoff_id    TEXT NOT NULL UNIQUE,          -- 固定交接号: 重试/重启不变, 省库据此去重
    status        TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'acked')),
    attempts      INTEGER NOT NULL DEFAULT 0,
    claimed_by    TEXT,                          -- 当前占用该修订的实例
    claim_until   REAL,                          -- 租约到期时间(同时充当"下次可重试时间")
    last_error    TEXT,
    receipt_json  TEXT,                          -- 省库回执原文
    created_at    REAL NOT NULL,
    last_sent_at  REAL,
    acked_at      REAL,
    UNIQUE (sample_no, revision_no)
);
CREATE INDEX IF NOT EXISTS idx_revisions_status ON revisions (status, sample_no, revision_no);
"""

# 每个样本只允许"最早的未确认修订"(头修订)进入发送队列 → 保证同一样本按修订顺序发送
HEAD_SQL = """
SELECT r.* FROM revisions r
WHERE r.status = 'pending'
  AND NOT EXISTS (
      SELECT 1 FROM revisions p
      WHERE p.sample_no = r.sample_no
        AND p.revision_no < r.revision_no
        AND p.status <> 'acked'
  )
  AND (r.claim_until IS NULL OR r.claim_until < ?)
ORDER BY r.id
LIMIT ?
"""

# 原子认领: 两个进程同时抢同一修订时, SQLite 串行化写事务, 只有一个能成功
CLAIM_SQL = """
UPDATE revisions
SET claimed_by = ?, claim_until = ?
WHERE id = ? AND status = 'pending' AND (claim_until IS NULL OR claim_until < ?)
"""

ACK_SQL = """
UPDATE revisions
SET status = 'acked', receipt_json = ?, acked_at = ?, last_sent_at = ?,
    attempts = attempts + 1, claimed_by = NULL, claim_until = NULL, last_error = NULL
WHERE id = ?
"""

FAIL_SQL = """
UPDATE revisions
SET attempts = attempts + 1, last_sent_at = ?, last_error = ?,
    claimed_by = NULL, claim_until = ?
WHERE id = ?
"""

# ---------------------------------------------------------------- SQLite 辅助(同步函数, 经 asyncio.to_thread 调用, 不阻塞事件循环)


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


def _insert_revision(sample_no: str, concentration: int) -> dict:
    """在同一事务内分配样本内递增修订号并落库。只有提交成功的修订才对发送器可见。"""
    now = time.time()
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        (rev_no,) = conn.execute(
            "SELECT COALESCE(MAX(revision_no), 0) + 1 FROM revisions WHERE sample_no = ?",
            (sample_no,),
        ).fetchone()
        handoff_id = f"{STATION_CODE}-{sample_no}-r{rev_no}-{secrets.token_hex(3)}"
        conn.execute(
            "INSERT INTO revisions (sample_no, revision_no, concentration, handoff_id, created_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (sample_no, rev_no, concentration, handoff_id, now),
        )
        row = conn.execute(
            "SELECT * FROM revisions WHERE handoff_id = ?", (handoff_id,)
        ).fetchone()
        conn.commit()
        return dict(row)
    finally:
        conn.close()


# ---------------------------------------------------------------- 后台发送器

_client: httpx.AsyncClient | None = None
_wakeup: asyncio.Event | None = None


async def send_one(rev: dict) -> None:
    """认领并发送一个头修订; 成功则落回执, 失败则退避后持续重试。"""
    now = time.time()
    claimed = await asyncio.to_thread(
        _execute, CLAIM_SQL, (INSTANCE_ID, now + CLAIM_LEASE_S, rev["id"], now)
    )
    if not claimed:
        return  # 另一实例已占用

    attempt = rev["attempts"] + 1
    payload = {
        "handoff_id": rev["handoff_id"],
        "station_code": STATION_CODE,
        "sample_no": rev["sample_no"],
        "revision_no": rev["revision_no"],
        "concentration": rev["concentration"],
        "unit": rev["unit"],
        "attempt": attempt,
        "sent_at": _iso(now),
    }
    try:
        resp = await _client.post(
            f"{PROVINCE_URL}/handoff", json=payload, timeout=SEND_TIMEOUT_S
        )
        if resp.status_code != 200:
            raise RuntimeError(f"province HTTP {resp.status_code}: {resp.text[:200]}")
        body = resp.json()
        if body.get("handoff_id") != rev["handoff_id"]:
            raise RuntimeError("receipt handoff_id mismatch")
        await asyncio.to_thread(
            _execute, ACK_SQL, (json.dumps(body, ensure_ascii=False), time.time(), now, rev["id"])
        )
        print(f"[{INSTANCE_ID}] ACK {rev['handoff_id']} attempt={attempt} receipt={body.get('receipt_id')}", flush=True)
    except Exception as exc:  # 断网/超时/5xx/丢回执 → 退避后持续重试
        backoff = min(1.0 * 2 ** min(attempt, 4), 15.0)
        await asyncio.to_thread(
            _execute,
            FAIL_SQL,
            (now, f"{type(exc).__name__}: {exc}"[:500], time.time() + backoff, rev["id"]),
        )
        print(f"[{INSTANCE_ID}] RETRY {rev['handoff_id']} attempt={attempt} in {backoff:.0f}s: {exc}", flush=True)


async def sender_loop() -> None:
    """扫描各样本的头修订并并行投递; 不同样本互不阻塞, 也不阻塞本地录入。"""
    sem = asyncio.Semaphore(MAX_INFLIGHT)

    async def run(rev: dict) -> None:
        try:
            await send_one(rev)
        except Exception as exc:  # 兜底: 单个修订异常不影响发送循环
            print(f"[{INSTANCE_ID}] send_one error id={rev['id']}: {exc}", flush=True)
        finally:
            sem.release()

    while True:
        try:
            heads = await asyncio.to_thread(_query, HEAD_SQL, (time.time(), MAX_INFLIGHT))
            for rev in heads:
                await sem.acquire()
                asyncio.create_task(run(rev))
        except Exception as exc:
            print(f"[{INSTANCE_ID}] sender loop error: {exc}", flush=True)
        try:
            await asyncio.wait_for(_wakeup.wait(), timeout=POLL_INTERVAL_S)
        except asyncio.TimeoutError:
            pass
        _wakeup.clear()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _client, _wakeup
    init_db()
    _client = httpx.AsyncClient()
    _wakeup = asyncio.Event()
    task = asyncio.create_task(sender_loop())  # 重启后自动续交: 扫描库里所有 pending 修订
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    await _client.aclose()


app = FastAPI(
    title="水质站总磷交接 API",
    version="1.0.0",
    description=(
        "LIMS 审核后的总磷结果交接至省级监测数仓。\n\n"
        "- 提交样本号 + 非负整数浓度(μg/L), 生成修订号与固定交接号\n"
        "- 只有本地保存成功的修订才会发送; 未确认的持续重试, 重启后续交\n"
        "- 同一样本按修订顺序发送, 不同样本并行"
    ),
    lifespan=lifespan,
)

# ---------------------------------------------------------------- 模型


class RevisionIn(BaseModel):
    sample_no: str = Field(pattern=r"^[A-Za-z0-9_-]{1,64}$", description="样本号", examples=["W2026-0001"])
    concentration: int = Field(ge=0, description="总磷浓度, 非负整数, 单位 μg/L", examples=[25])


class RevisionOut(BaseModel):
    id: int
    sample_no: str
    revision_no: int
    concentration: int
    unit: str
    handoff_id: str
    status: str
    attempts: int
    claimed_by: str | None
    last_error: str | None
    receipt: dict | None
    created_at: str | None
    last_sent_at: str | None
    acked_at: str | None


# ---------------------------------------------------------------- 工具


def _iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _shape(row: dict) -> dict:
    return {
        "id": row["id"],
        "sample_no": row["sample_no"],
        "revision_no": row["revision_no"],
        "concentration": row["concentration"],
        "unit": row["unit"],
        "handoff_id": row["handoff_id"],
        "status": row["status"],
        "attempts": row["attempts"],
        "claimed_by": row["claimed_by"],
        "last_error": row["last_error"],
        "receipt": json.loads(row["receipt_json"]) if row["receipt_json"] else None,
        "created_at": _iso(row["created_at"]),
        "last_sent_at": _iso(row["last_sent_at"]),
        "acked_at": _iso(row["acked_at"]),
    }


# ---------------------------------------------------------------- 路由


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse("/docs")


@app.post("/revisions", response_model=RevisionOut, status_code=201, tags=["revisions"],
          summary="录入一个修订(样本号 + 非负整数浓度 μg/L)")
async def create_revision(body: RevisionIn):
    row = await asyncio.to_thread(_insert_revision, body.sample_no, body.concentration)
    if _wakeup is not None:
        _wakeup.set()  # 立即唤醒发送器, 不必等下一个轮询周期
    return _shape(row)


@app.get("/revisions", response_model=list[RevisionOut], tags=["revisions"],
         summary="查询各修订及交接状态")
async def list_revisions(
    sample_no: str | None = None,
    status: str | None = Query(None, pattern="^(pending|acked)$"),
):
    sql, params, conds = "SELECT * FROM revisions", [], []
    if sample_no:
        conds.append("sample_no = ?")
        params.append(sample_no)
    if status:
        conds.append("status = ?")
        params.append(status)
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY id"
    rows = await asyncio.to_thread(_query, sql, tuple(params))
    return [_shape(r) for r in rows]


@app.get("/revisions/{handoff_id}", response_model=RevisionOut, tags=["revisions"],
         summary="按交接号查询单个修订")
async def get_revision(handoff_id: str):
    rows = await asyncio.to_thread(
        _query, "SELECT * FROM revisions WHERE handoff_id = ?", (handoff_id,)
    )
    if not rows:
        raise HTTPException(404, "handoff_id not found")
    return _shape(rows[0])


@app.get("/samples/{sample_no}/revisions", response_model=list[RevisionOut], tags=["revisions"],
         summary="查询某样本的全部修订(按修订号排序)")
async def list_sample_revisions(sample_no: str):
    rows = await asyncio.to_thread(
        _query,
        "SELECT * FROM revisions WHERE sample_no = ? ORDER BY revision_no",
        (sample_no,),
    )
    return [_shape(r) for r in rows]


@app.get("/status", tags=["status"], summary="本实例与交接概况")
async def status():
    rows = await asyncio.to_thread(
        _query, "SELECT status, COUNT(*) AS n FROM revisions GROUP BY status"
    )
    counts = {r["status"]: r["n"] for r in rows}
    return {
        "instance": INSTANCE_ID,
        "station_code": STATION_CODE,
        "province_url": PROVINCE_URL,
        "pending": counts.get("pending", 0),
        "acked": counts.get("acked", 0),
    }


@app.get("/health", include_in_schema=False)
async def health():
    return {"ok": True}
