"""
站内交接服务 (station)

- LIMS 审核后的总磷结果录入为"修订"(revision): 样本号 + 非负整数浓度(μg/L)
- 每个修订生成样本内递增的修订号(revision_no)和固定交接号(handoff_id)
- 只有成功落库(事务提交)的修订才会被后台发送器投递到省库
- 未获收妥确认的修订持续重试(指数退避); 进程重启后自动续交
- 同一样本严格按修订顺序发送(前一份未确认, 下一份不发送); 不同样本并行
- 两个服务进程共用同一 SQLite 库, 通过租约(claim)协调占用, 不会重复发送同一修订;
  即便极端情况下重复发送, 省库按交接号去重也会返回原回执, 因此端到端安全

恢复模式(RECOVERY_MODE=1):
- 数据卷损坏后, 在空白卷上以恢复模式启动, 通过 POST /recovery 上传同版、同站的
  SQLite 备份; 站内再从省库读取已收记录, 合并回备份缺少的修订/浓度/交接号/回执号
- 恢复状态持久在共享 SQLite 中: 两个站内进程据此同时封闭录入与发送
- 恢复结果在单个写事务中提交, 查询不会看到半恢复的修订
- 冲突或任何读写失败都保持封闭; 重启后可重试; 成功后新修订号从历史最大值接续
"""

import asyncio
import hashlib
import json
import os
import shutil
import secrets
import sqlite3
import tempfile
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import httpx
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

import recovery

# ---------------------------------------------------------------- 配置

DB_PATH = os.environ.get("STATION_DB", "station.db")
PROVINCE_URL = os.environ.get("PROVINCE_URL", "http://localhost:8000").rstrip("/")
INSTANCE_ID = os.environ.get("INSTANCE_ID", f"station-{os.getpid()}")
STATION_CODE = os.environ.get("STATION_CODE", "ST01")
SEND_TIMEOUT_S = float(os.environ.get("SEND_TIMEOUT_S", "5"))      # 单次发送超时
POLL_INTERVAL_S = float(os.environ.get("POLL_INTERVAL_S", "1.0"))  # 发送器轮询间隔
CLAIM_LEASE_S = float(os.environ.get("CLAIM_LEASE_S", "15"))       # 认领租约时长(进程崩溃后租约过期, 由其他进程接管)
MAX_INFLIGHT = int(os.environ.get("MAX_INFLIGHT", "50"))           # 单进程并发发送上限
RECOVERY_MODE = os.environ.get("RECOVERY_MODE", "0").strip().lower() in {"1", "true", "yes", "on"}
RECOVERY_TIMEOUT_S = float(os.environ.get("RECOVERY_TIMEOUT_S", "30"))  # 恢复时读取省库的超时
MAX_BACKUP_BYTES = int(os.environ.get("MAX_BACKUP_BYTES", str(512 * 1024 * 1024)))

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
    # 两个进程可能同时初始化/打开同一库; SQLite 的 WAL 切换需要短暂排他锁。
    last_exc: Exception | None = None
    for attempt in range(10):
        conn = sqlite3.connect(DB_PATH, timeout=15)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=15000")
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            return conn
        except sqlite3.OperationalError as exc:  # database is locked: 短暂退避后重试
            conn.close()
            last_exc = exc
            time.sleep(0.2 * (attempt + 1))
    raise last_exc  # type: ignore[misc]


def init_db() -> None:
    d = os.path.dirname(DB_PATH)
    if d:
        os.makedirs(d, exist_ok=True)
    conn = _connect()
    try:
        conn.executescript(SCHEMA)
        recovery.startup_migrate(conn, RECOVERY_MODE, STATION_CODE)
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


def _require_open(conn: sqlite3.Connection) -> None:
    """恢复未完成时封闭录入、发送与查询(两个进程共用同一持久状态)。"""
    state = recovery.observed_state(conn, RECOVERY_MODE)
    if state not in recovery.OPEN_STATES:
        raise HTTPException(
            status_code=423,
            detail={
                "error": "recovery_in_progress" if state == "recovering" else "station_locked",
                "state": state,
                "message": "恢复数据尚未完整生效, 录入与查询保持封闭"
                if state == "recovering" else "恢复失败后保持封闭, 请重试恢复",
            },
        )


def _gate_open() -> None:
    conn = _connect()
    try:
        _require_open(conn)
    finally:
        conn.close()


def _insert_revision(sample_no: str, concentration: int) -> dict:
    """在同一事务内分配样本内递增修订号并落库。只有提交成功的修订才对发送器可见。"""
    now = time.time()
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        _require_open(conn)  # 与写入同一写事务, 恢复期间不会插入任何修订
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


def _reset_pending_retry() -> int:
    """恢复失败后重试: 清掉退避, 让发送器立即重新扫描待交接修订。"""
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        state = recovery.observed_state(conn, RECOVERY_MODE)
        if state == "recovering":
            raise HTTPException(423, {"error": "recovery_in_progress", "state": state})
        if state != "failed":
            raise HTTPException(409, {"error": "no_failed_recovery", "state": state})
        cur = conn.execute(
            "UPDATE revisions SET claimed_by = NULL, claim_until = NULL, last_error = NULL"
            " WHERE status = 'pending'"
        )
        conn.commit()
        return cur.rowcount
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
            open_ = await asyncio.to_thread(_is_open)
            if open_:
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


def _is_open() -> bool:
    conn = _connect()
    try:
        return recovery.is_open(conn, RECOVERY_MODE)
    finally:
        conn.close()


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
    version="1.1.0",
    description=(
        "LIMS 审核后的总磷结果交接至省级监测数仓。\n\n"
        "- 提交样本号 + 非负整数浓度(μg/L), 生成修订号与固定交接号\n"
        "- 只有本地保存成功的修订才会发送; 未确认的持续重试, 重启后续交\n"
        "- 同一样本按修订顺序发送, 不同样本并行\n\n"
        "恢复(需以 `RECOVERY_MODE=1` 在空白数据卷上启动):\n"
        "- `POST /recovery` 上传同版、同站 SQLite 备份, 从省库补回缺少的修订与回执\n"
        "- 恢复完成前, 录入与查询返回 423; 恢复结果原子生效\n"
        "- `GET /recovery/status` 查看进度; 失败后 `POST /recovery/retry` 重试"
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


class ConflictOut(BaseModel):
    sample_no: str
    revision_no: int
    field: str
    backup: str | int | None
    province: str | int | None


class RecoveryResult(BaseModel):
    recovery_state: str
    revision_count: int
    sample_count: int
    station_code: str
    source_backup: str | None
    recovered_at: str
    from_backup_only: int
    from_province_only: int
    matched: int
    acked: int
    pending: int
    conflicts: int


class RecoveryStatusOut(BaseModel):
    mode: str
    state: str
    open: bool
    busy: bool
    current_attempt: str | None
    last_attempt: str | None
    last_error: str | None
    last_result: dict | None
    completed_at: float | None


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


def _status_view() -> dict:
    conn = _connect()
    try:
        return recovery.state_status(conn, RECOVERY_MODE)
    finally:
        conn.close()


async def _fetch_province_receipts() -> list[dict]:
    """恢复期间省库没有其他写入, 一次快照读取即可代表完整已收记录。"""
    try:
        resp = await _client.get(f"{PROVINCE_URL}/receipts", timeout=RECOVERY_TIMEOUT_S)
    except httpx.HTTPError as exc:
        raise recovery.RecoveryError(f"读取省库失败: {type(exc).__name__}: {exc}") from exc
    if resp.status_code != 200:
        raise recovery.RecoveryError(f"读取省库失败: HTTP {resp.status_code}: {resp.text[:200]}")
    try:
        rows = resp.json()
    except ValueError as exc:
        raise recovery.RecoveryError(f"省库记录不是合法 JSON: {exc}") from exc
    if not isinstance(rows, list):
        raise recovery.RecoveryError("省库记录格式错误")
    for r in rows:
        if not isinstance(r, dict) or not all(
            k in r for k in ("handoff_id", "sample_no", "revision_no", "concentration",
                             "receipt_id", "received_at")
        ):
            raise recovery.RecoveryError("省库记录缺少必要字段")
        if "received_seq" in r:
            r["seq"] = r["received_seq"]
        elif "seq" not in r:
            raise recovery.RecoveryError("省库记录缺少接收顺序号")
    return rows


def _begin_recovery(filename: str, size: int, sha256: str) -> str:
    conn = recovery.connect(DB_PATH)
    try:
        return recovery.begin_attempt(
            conn, RECOVERY_MODE, INSTANCE_ID, filename, size, sha256, STATION_CODE
        )
    finally:
        conn.close()


def _load_backup(path: str, workdir: str) -> dict:
    loaded = recovery.load_backup(
        Path(path), workdir, STATION_CODE, recovery.REQUIRED_REVISION_COLUMNS
    )
    return loaded


def _merge_plan(loaded: dict, province_rows: list[dict], filename: str) -> dict:
    plan = recovery.merge(loaded, province_rows, STATION_CODE, filename)
    if plan["conflicts"]:
        raise recovery.RecoveryConflict(plan["conflicts"])
    return plan


def _commit_recovery(attempt_id: str, plan: dict) -> dict:
    conn = recovery.connect(DB_PATH)
    try:
        return recovery.commit_plan(conn, attempt_id, plan, STATION_CODE)
    finally:
        conn.close()


def _mark_failed(attempt_id: str, error: str, extra: dict | None = None) -> None:
    conn = recovery.connect(DB_PATH)
    try:
        recovery.mark_failed(conn, attempt_id, error, extra)
    finally:
        conn.close()


def _touch_attempt(attempt_id: str) -> bool:
    conn = recovery.connect(DB_PATH)
    try:
        return recovery.touch_attempt(conn, attempt_id)
    finally:
        conn.close()


def _recovery_status() -> dict:
    return _status_view()


# ---------------------------------------------------------------- 路由


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse("/docs")


@app.post("/revisions", response_model=RevisionOut, status_code=201, tags=["revisions"],
          summary="录入一个修订(样本号 + 非负整数浓度 μg/L)")
async def create_revision(body: RevisionIn):
    try:
        row = await asyncio.to_thread(_insert_revision, body.sample_no, body.concentration)
    except HTTPException:
        raise
    if _wakeup is not None:
        _wakeup.set()  # 立即唤醒发送器, 不必等下一个轮询周期
    return _shape(row)


@app.get("/revisions", response_model=list[RevisionOut], tags=["revisions"],
         summary="查询各修订及交接状态(恢复未完成时返回 423)")
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

    def run():
        conn = _connect()
        try:
            _require_open(conn)  # 与读取同一连接, 不会返回半恢复的修订
            return [dict(r) for r in conn.execute(sql, tuple(params)).fetchall()]
        finally:
            conn.close()

    rows = await asyncio.to_thread(run)
    return [_shape(r) for r in rows]


@app.get("/revisions/{handoff_id}", response_model=RevisionOut, tags=["revisions"],
         summary="按交接号查询单个修订(恢复未完成时返回 423)")
async def get_revision(handoff_id: str):
    def run():
        conn = _connect()
        try:
            _require_open(conn)
            return [dict(r) for r in conn.execute(
                "SELECT * FROM revisions WHERE handoff_id = ?", (handoff_id,)
            ).fetchall()]
        finally:
            conn.close()

    rows = await asyncio.to_thread(run)
    if not rows:
        raise HTTPException(404, "handoff_id not found")
    return _shape(rows[0])


@app.get("/samples/{sample_no}/revisions", response_model=list[RevisionOut], tags=["revisions"],
         summary="查询某样本的全部修订(按修订号排序; 恢复未完成时返回 423)")
async def list_sample_revisions(sample_no: str):
    def run():
        conn = _connect()
        try:
            _require_open(conn)
            return [dict(r) for r in conn.execute(
                "SELECT * FROM revisions WHERE sample_no = ? ORDER BY revision_no",
                (sample_no,),
            ).fetchall()]
        finally:
            conn.close()

    rows = await asyncio.to_thread(run)
    return [_shape(r) for r in rows]


@app.get("/status", tags=["status"], summary="本实例、交接与恢复概况")
async def status():
    def run():
        conn = _connect()
        try:
            counts = {r["status"]: r["n"] for r in conn.execute(
                "SELECT status, COUNT(*) AS n FROM revisions GROUP BY status"
            ).fetchall()}
            rec = recovery.state_status(conn, RECOVERY_MODE)
        finally:
            conn.close()
        return counts, rec

    counts, rec = await asyncio.to_thread(run)
    return {
        "instance": INSTANCE_ID,
        "station_code": STATION_CODE,
        "province_url": PROVINCE_URL,
        "pending": counts.get("pending", 0),
        "acked": counts.get("acked", 0),
        "open": rec["open"],
        "recovery": rec,
    }


@app.get("/recovery/status", response_model=RecoveryStatusOut, tags=["recovery"],
         summary="查看恢复状态(不受恢复封闭影响)")
async def recovery_status():
    return await asyncio.to_thread(_recovery_status)


@app.post("/recovery", response_model=RecoveryResult, tags=["recovery"],
          summary="上传同版同站 SQLite 备份并从省库重建交接进度(需恢复模式)")
async def run_recovery(backup: UploadFile = File(...)):
    """恢复流程: 备份校验 → 读取省库 → 合并 → 单个事务提交。

    任一步失败(备份损坏、省库不可读、冲突、进程退出)都保持封闭,
    重启后可再次调用重试; 响应返回 409/423/502 并说明原因。
    """
    workdir = tempfile.mkdtemp(prefix="tp-recovery-")
    attempt_id = None
    try:
        upload_path = Path(workdir) / "upload.bin"
        size = 0
        digest = hashlib.sha256()
        try:
            with open(upload_path, "wb") as f:
                while True:
                    chunk = await backup.read(1024 * 1024)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > MAX_BACKUP_BYTES:
                        raise HTTPException(413, "备份文件过大")
                    digest.update(chunk)
                    f.write(chunk)
        except HTTPException:
            raise
        except OSError as exc:
            raise HTTPException(400, f"读取上传文件失败: {exc}")
        if size == 0:
            raise HTTPException(400, "备份文件为空")
        filename = backup.filename or "backup.db"
        sha256 = digest.hexdigest()

        try:
            attempt_id = await asyncio.to_thread(_begin_recovery, filename, size, sha256)
        except recovery.RecoveryError as exc:
            raise HTTPException(409, {"error": "recovery_rejected", "message": str(exc)})

        try:
            loaded = await asyncio.to_thread(_load_backup, str(upload_path), workdir)
            await asyncio.to_thread(_touch_attempt, attempt_id)  # 长备份读取期间续租
            province_rows = await _fetch_province_receipts()
            await asyncio.to_thread(_touch_attempt, attempt_id)  # 省库读取期间续租
            plan = await asyncio.to_thread(_merge_plan, loaded, province_rows, filename)
        except recovery.RecoveryConflict as exc:
            await asyncio.to_thread(
                _mark_failed, attempt_id, "合并冲突: 交接号或浓度不一致",
                {"conflicts": exc.conflicts},
            )
            raise HTTPException(409, {
                "error": "recovery_conflict",
                "message": "同一样本同一修订号的交接号或浓度不一致, 未覆盖任何一方",
                "conflicts": exc.conflicts,
                "state": "failed",
                "retryable": True,
            })
        except recovery.RecoveryError as exc:
            await asyncio.to_thread(_mark_failed, attempt_id, str(exc))
            raise HTTPException(409, {"error": "recovery_failed", "message": str(exc),
                                      "state": "failed", "retryable": True})
        except HTTPException:
            raise
        except Exception as exc:
            await asyncio.to_thread(_mark_failed, attempt_id, f"{type(exc).__name__}: {exc}")
            raise HTTPException(502, {
                "error": "recovery_failed",
                "message": f"读取备份/省库失败: {type(exc).__name__}: {exc}",
                "state": "failed", "retryable": True,
            })

        try:
            result = await asyncio.to_thread(_commit_recovery, attempt_id, plan)
        except recovery.RecoveryConflict as exc:
            await asyncio.to_thread(
                _mark_failed, attempt_id, "合并冲突: 交接号或浓度不一致",
                {"conflicts": exc.conflicts},
            )
            raise HTTPException(409, {
                "error": "recovery_conflict",
                "message": "同一样本同一修订号的交接号或浓度不一致, 未覆盖任何一方",
                "conflicts": exc.conflicts,
                "state": "failed",
                "retryable": True,
            })
        except recovery.RecoveryError as exc:
            await asyncio.to_thread(_mark_failed, attempt_id, str(exc))
            raise HTTPException(409, {"error": "recovery_failed", "message": str(exc),
                                      "state": "failed", "retryable": True})
        except Exception as exc:
            await asyncio.to_thread(_mark_failed, attempt_id, f"{type(exc).__name__}: {exc}")
            raise HTTPException(500, {
                "error": "recovery_failed",
                "message": f"提交恢复数据失败: {type(exc).__name__}: {exc}",
                "state": "failed", "retryable": True,
            })

        if _wakeup is not None:
            _wakeup.set()  # 开放后立即续交备份/省库中仍是 pending 的修订
        out = {"recovery_state": "ready"}
        out.update(result)
        return out
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


@app.post("/recovery/retry", tags=["recovery"],
          summary="恢复失败后重试(清退避, 重新开放发送器)")
async def retry_recovery():
    try:
        count = await asyncio.to_thread(_reset_pending_retry)
    except HTTPException:
        raise
    if _wakeup is not None:
        _wakeup.set()
    return {"state": "ready", "resumed_pending": count, "open": True}


@app.get("/health", include_in_schema=False)
async def health():
    try:
        state = (await asyncio.to_thread(_is_open))
        view = await asyncio.to_thread(_recovery_status)
        return {"ok": True, "open": state, "recovery_state": view["state"]}
    except Exception:
        return {"ok": False}
