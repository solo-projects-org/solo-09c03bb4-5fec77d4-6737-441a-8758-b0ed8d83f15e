"""
站内恢复(recovery)辅助模块

恢复流程:
1. 空白数据卷以 RECOVERY_MODE=1 启动, 数据库中的 recovery_state 进入 recovering,
   两个进程据此封闭录入与发送。
2. 上传同版、同站的 SQLite 备份; 服务端只读打开备份副本, 校验完整性和表结构。
3. 从省库读取已接收记录, 与备份按 (样本号, 修订号) 合并。
4. 同一样本同一修订号的交接号或浓度不一致 -> 冲突, 整体拒绝, 不覆盖任一方。
5. 合并结果在一个 SQLite 写事务中提交: 要么全部可见, 要么全部不可见。
6. 成功后状态改为 ready, 两个进程开放录入与发送; 新修订号由 MAX(修订号)+1 接续。

所有函数都是同步的, 由 app.py 通过 asyncio.to_thread 调用。
"""

import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

# 恢复状态表与元数据表; 与 revisions 放在同一个库里, 两个站内进程共享
RECOVERY_SCHEMA = """
CREATE TABLE IF NOT EXISTS recovery_state (
    id                INTEGER PRIMARY KEY CHECK (id = 1),
    state             TEXT NOT NULL DEFAULT 'closed'
                      CHECK (state IN ('closed', 'recovering', 'ready', 'failed')),
    current_attempt   TEXT,                -- 正在执行的恢复尝试 id
    attempt_owner     TEXT,                -- 发起该尝试的实例(重启后同实例可接管)
    attempt_lease_until REAL,              -- 尝试租约: 进程中途退出后租约过期, 可重试
    last_attempt      TEXT,                -- 最近一次恢复尝试 id
    last_error        TEXT,                -- 最近一次失败原因
    last_result       TEXT,                -- 最近一次成功后的统计(JSON)
    updated_at        REAL NOT NULL,
    completed_at      REAL                 -- 最近一次成功恢复完成时间
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

OPEN_STATES = ("closed", "ready")      # 录入/发送/查询开放的状态
CLOSED_STATES = ("recovering", "failed")  # 恢复中或恢复失败, 保持封闭

# 备份必须包含的核心列(同版校验); 允许备份缺少后续新增的辅助表
REQUIRED_REVISION_COLUMNS = (
    "sample_no", "revision_no", "concentration", "unit",
    "handoff_id", "status", "attempts", "receipt_json", "created_at",
    "last_sent_at", "acked_at",
)

# 单次恢复尝试的接管租约。进程中途退出时租约无法续期, 到期后另一个进程可重试;
# 同一实例(同一 INSTANCE_ID)重启后的重试可立即接管自己的旧尝试。
ATTEMPT_LEASE_S = float(os.environ.get("RECOVERY_ATTEMPT_LEASE_S", "20"))


class RecoveryConflict(Exception):
    """合并时发现冲突: 整体拒绝, 不覆盖任一方。"""

    def __init__(self, conflicts: list[dict]):
        self.conflicts = conflicts
        super().__init__(f"{len(conflicts)} conflict(s)")


class RecoveryError(Exception):
    """恢复前置条件/备份/省库读取失败。"""


def _now() -> float:
    return time.time()


def _iso(value) -> str | None:
    ts = _epoch(value)
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def connect(db_path: str, read_only: bool = False) -> sqlite3.Connection:
    if read_only:
        uri = f"file:{quote(str(Path(db_path).resolve()))}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=30)
        conn.execute("PRAGMA query_only=ON")
    else:
        last_exc: Exception | None = None
        for attempt in range(10):
            conn = sqlite3.connect(db_path, timeout=30)
            conn.execute("PRAGMA busy_timeout=30000")
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                break
            except sqlite3.OperationalError as exc:  # database is locked: 短暂退避后重试
                conn.close()
                last_exc = exc
                time.sleep(0.2 * (attempt + 1))
        else:
            raise last_exc  # type: ignore[misc]
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------- 状态


def observed_state(conn: sqlite3.Connection, recovery_mode: bool) -> str:
    """读取共享库中的恢复状态。

    没有状态行时: 恢复模式视为 recovering(尚未恢复), 普通模式视为 closed。
    这样任一进程都能基于同一份持久状态做出封闭判断。
    """
    try:
        row = conn.execute("SELECT state FROM recovery_state WHERE id = 1").fetchone()
    except sqlite3.OperationalError:
        row = None
    if row is not None:
        return row["state"]
    return "recovering" if recovery_mode else "closed"


def is_open(conn: sqlite3.Connection, recovery_mode: bool) -> bool:
    return observed_state(conn, recovery_mode) in OPEN_STATES


def startup_migrate(conn: sqlite3.Connection, recovery_mode: bool, station_code: str) -> None:
    """建表并写入初始状态。

    - 恢复模式: 空白库里尚无状态 -> recovering(尚无进行中的尝试); 已有 closed/failed ->
      重新进入 recovering。不打断另一进程正在进行的 recovering, 也不回退已完成的 ready。
    - 普通模式: 仅在无状态行时写入 closed; 不动已有的 recovering/failed/ready。
    """
    conn.executescript(RECOVERY_SCHEMA)
    now = _now()
    conn.execute(
        "INSERT OR IGNORE INTO recovery_state (id, state, updated_at) VALUES (1, ?, ?)",
        ("recovering" if recovery_mode else "closed", now),
    )
    if recovery_mode:
        conn.execute(
            "UPDATE recovery_state SET state = 'recovering', current_attempt = NULL,"
            " attempt_owner = NULL, attempt_lease_until = NULL, last_error = NULL, updated_at = ?"
            " WHERE id = 1 AND state IN ('closed', 'failed')",
            (now,),
        )
    conn.execute(
        "INSERT OR IGNORE INTO meta (key, value) VALUES ('station_code', ?)",
        (station_code,),
    )
    conn.commit()


def state_status(conn: sqlite3.Connection, recovery_mode: bool) -> dict:
    row = conn.execute("SELECT * FROM recovery_state WHERE id = 1").fetchone()
    state = observed_state(conn, recovery_mode)
    result = json.loads(row["last_result"]) if row is not None and row["last_result"] else None
    return {
        "mode": "recovery" if recovery_mode else "normal",
        "state": state,
        "open": state in OPEN_STATES,
        "busy": state == "recovering" and row is not None and row["current_attempt"] is not None,
        "current_attempt": row["current_attempt"] if row is not None else None,
        "last_attempt": row["last_attempt"] if row is not None else None,
        "last_error": row["last_error"] if row is not None else None,
        "last_result": result,
        "completed_at": row["completed_at"] if row is not None else None,
    }


def begin_attempt(
    conn: sqlite3.Connection, recovery_mode: bool, owner: str,
    filename: str, size: int, sha256: str, station_code: str,
) -> str:
    """登记一次恢复尝试。空白卷校验、并发尝试接管与备份元数据记录都在同一写事务内完成。

    另一进程正在恢复且租约未过期时拒绝(409); 同一实例重启或租约已过期则可以接管。
    """
    now = _now()
    attempt_id = f"rcv-{int(now * 1000)}-{os.urandom(3).hex()}"
    conn.execute("BEGIN IMMEDIATE")
    try:
        if not recovery_mode:
            raise RecoveryError("服务未以恢复模式启动 (RECOVERY_MODE=1)")
        state = observed_state(conn, recovery_mode)
        if state == "ready":
            raise RecoveryError("恢复已完成, 无需重复恢复")
        if state == "closed":
            # 只有普通模式会写 closed; 恢复模式启动时应已改为 recovering
            raise RecoveryError("数据卷不是空白恢复状态, 拒绝恢复")
        row = conn.execute(
            "SELECT current_attempt, attempt_owner, attempt_lease_until FROM recovery_state"
            " WHERE id = 1"
        ).fetchone()
        if row is not None and row["current_attempt"]:
            lease_until = float(row["attempt_lease_until"] or 0)
            if row["attempt_owner"] != owner and now < lease_until:
                raise RecoveryError("已有恢复正在进行, 请等待或稍后重试")
        (n,) = conn.execute("SELECT COUNT(*) FROM revisions").fetchone()
        if n:
            raise RecoveryError("数据卷上已有修订, 恢复只能在空白卷上执行")
        conn.execute(
            "UPDATE recovery_state SET state = 'recovering', current_attempt = ?,"
            " attempt_owner = ?, attempt_lease_until = ?, last_attempt = ?,"
            " last_error = NULL, updated_at = ? WHERE id = 1",
            (attempt_id, owner, now + ATTEMPT_LEASE_S, attempt_id, now),
        )
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('last_backup', ?)"
            " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (json.dumps({"filename": filename, "size": size, "sha256": sha256,
                         "station_code": station_code, "uploaded_at": now}, ensure_ascii=False),),
        )
        conn.commit()
        return attempt_id
    except Exception:
        conn.rollback()
        raise


def touch_attempt(conn: sqlite3.Connection, attempt_id: str) -> bool:
    """为进行中的尝试续租; 尝试已被接管则返回 False。"""
    cur = conn.execute(
        "UPDATE recovery_state SET attempt_lease_until = ?, updated_at = ?"
        " WHERE id = 1 AND state = 'recovering' AND current_attempt = ?",
        (_now() + ATTEMPT_LEASE_S, _now(), attempt_id),
    )
    conn.commit()
    return cur.rowcount == 1


def is_current_attempt(conn: sqlite3.Connection, attempt_id: str) -> bool:
    row = conn.execute("SELECT current_attempt FROM recovery_state WHERE id = 1").fetchone()
    return row is not None and row["current_attempt"] == attempt_id


def mark_failed(conn: sqlite3.Connection, attempt_id: str, error: str,
                extra: dict | None = None) -> None:
    """记录失败并保持封闭; 只有当前尝试能改写状态, 避免并发尝试互相覆盖。"""
    now = _now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute("SELECT state, current_attempt FROM recovery_state WHERE id = 1").fetchone()
        if row is None or row["current_attempt"] != attempt_id or row["state"] == "ready":
            conn.rollback()
            return
        payload = json.dumps(extra, ensure_ascii=False) if extra else None
        conn.execute(
            "UPDATE recovery_state SET state = 'failed', current_attempt = NULL,"
            " last_error = ?, last_result = ?, updated_at = ? WHERE id = 1",
            (error[:2000], payload, now),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def _insert_revision_rows(conn: sqlite3.Connection, rows: list[dict]) -> None:
    conn.executemany(
        "INSERT INTO revisions (sample_no, revision_no, concentration, unit, handoff_id,"
        " status, attempts, claimed_by, claim_until, last_error, receipt_json, created_at,"
        " last_sent_at, acked_at) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, ?, ?)",
        [
            (
                r["sample_no"], r["revision_no"], r["concentration"], r["unit"],
                r["handoff_id"], r["status"], r["attempts"], r["receipt_json"],
                r["created_at"], r["last_sent_at"], r["acked_at"],
            )
            for r in rows
        ],
    )


def commit_plan(conn: sqlite3.Connection, attempt_id: str, plan: dict,
                station_code: str) -> dict:
    """在单个写事务内落地恢复结果。

    提交前再次核对尝试 id 与冲突集; 任何异常都回滚, 查询看不到半恢复的修订。
    """
    now = _now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute("SELECT state, current_attempt FROM recovery_state WHERE id = 1").fetchone()
        if row is None or row["current_attempt"] != attempt_id or row["state"] not in CLOSED_STATES:
            raise RecoveryError("恢复尝试已失效, 请重新发起")
        (n,) = conn.execute("SELECT COUNT(*) FROM revisions").fetchone()
        if n:
            raise RecoveryError("数据卷上已有修订, 拒绝覆盖")
        if plan["conflicts"]:
            raise RecoveryConflict(plan["conflicts"])
        _insert_revision_rows(conn, plan["rows"])
        result = dict(plan["stats"])
        conn.execute(
            "UPDATE recovery_state SET state = 'ready', current_attempt = NULL,"
            " last_error = NULL, last_result = ?, updated_at = ?, completed_at = ? WHERE id = 1",
            (json.dumps(result, ensure_ascii=False), now, now),
        )
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('station_code', ?)"
            " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (station_code,),
        )
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise


# ---------------------------------------------------------------- 备份读取


def _open_backup_copy(src: Path, workdir: str) -> sqlite3.Connection:
    """把上传文件复制到独立目录后只读打开, 不触碰原始文件, 也不在用户的目录里建辅助文件。"""
    dst = Path(workdir) / "backup.db"
    with open(src, "rb") as fsrc, open(dst, "wb") as fdst:
        while True:
            chunk = fsrc.read(1024 * 1024)
            if not chunk:
                break
            fdst.write(chunk)
    return connect(str(dst), read_only=True)


def load_backup(src: Path, workdir: str, station_code: str, required_columns: tuple) -> dict:
    """校验并读取同版、同站的备份, 返回样本集合与修订列表。"""
    try:
        conn = _open_backup_copy(src, workdir)
    except sqlite3.DatabaseError as exc:
        raise RecoveryError(f"备份不是有效的 SQLite 文件: {exc}") from exc
    try:
        try:
            check = conn.execute("PRAGMA integrity_check").fetchall()
        except sqlite3.DatabaseError as exc:
            raise RecoveryError(f"备份读取失败: {exc}") from exc
        if [r[0] for r in check] != ["ok"]:
            raise RecoveryError("备份完整性校验失败: " + "; ".join(str(r[0]) for r in check))

        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )}
        if "revisions" not in tables:
            raise RecoveryError("备份缺少 revisions 表, 不是本系统的备份")
        cols = {r[1] for r in conn.execute("PRAGMA table_info(revisions)")}
        missing = [c for c in required_columns if c not in cols]
        if missing:
            raise RecoveryError("备份与当前版本不一致, 缺少列: " + ", ".join(missing))

        meta_station = None
        if "meta" in tables:
            row = conn.execute("SELECT value FROM meta WHERE key = 'station_code'").fetchone()
            meta_station = row[0] if row else None
        if meta_station and meta_station != station_code:
            raise RecoveryError(
                f"备份属于其他站点({meta_station}), 当前站点为 {station_code}"
            )

        try:
            rows = [dict(r) for r in conn.execute(
                "SELECT sample_no, revision_no, concentration, unit, handoff_id,"
                " status, attempts, receipt_json, created_at, last_sent_at, acked_at"
                " FROM revisions ORDER BY sample_no, revision_no"
            )]
        except sqlite3.DatabaseError as exc:
            raise RecoveryError(f"备份读取失败: {exc}") from exc
    finally:
        conn.close()

    backup_rows: dict[tuple[str, int], dict] = {}
    samples: set[str] = set()
    for r in rows:
        sample = r["sample_no"]
        rev = r["revision_no"]
        conc = r["concentration"]
        if not isinstance(sample, str) or not sample:
            raise RecoveryError("备份含非法样本号")
        if not isinstance(rev, int) or isinstance(rev, bool) or rev < 1:
            raise RecoveryError(f"备份含非法修订号: {sample}/{rev!r}")
        if not isinstance(conc, int) or isinstance(conc, bool) or conc < 0:
            raise RecoveryError(f"备份含非法浓度: {sample}/r{rev}={conc!r}")
        if not isinstance(r["handoff_id"], str) or not r["handoff_id"]:
            raise RecoveryError(f"备份含空交接号: {sample}/r{rev}")
        key = (sample, rev)
        if key in backup_rows:
            raise RecoveryError(f"备份含重复修订: {sample} 修订号 {rev}")
        backup_rows[key] = r
        samples.add(sample)
    return {"rows": backup_rows, "samples": samples, "revision_count": len(backup_rows)}


# ---------------------------------------------------------------- 与省库记录合并


def _epoch(value) -> float | None:
    """把时间值规范成 epoch 秒; 接受数字或 ISO 8601 字符串。"""
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None
    return None


def build_receipt(province_row: dict) -> dict:
    """把省库记录还原成站内原有的回执形状, 保证恢复后回查口径一致。"""
    return {
        "receipt_id": province_row["receipt_id"],
        "handoff_id": province_row["handoff_id"],
        "status": "ACCEPTED",
        "duplicate": False,
        "received_seq": province_row["seq"],
        "received_at": _iso(province_row["received_at"]),
        "recovered_from_province": True,
    }


def merge(
    backup: dict,
    province_rows: list[dict],
    station_code: str,
    source_backup: str | None,
) -> dict:
    """合并备份与省库记录, 产出待提交的行与冲突。

    规则:
    - 备份中有、省库无: 按备份中的交接进度保留(已确认的保持已确认, 其余按原交接号续交)
    - 省库有、备份无: 补回修订、浓度、交接号和回执号; 标为已确认
    - 两边都有: 交接号与浓度必须一致; 一致时进度取较新的(省库已收即已确认)
    - 同一样本同一修订号交接号或浓度不一致: 冲突, 整体拒绝
    - 备份和省库都没有的: 不推造
    """
    backup_rows: dict[tuple[str, int], dict] = backup["rows"]
    samples: set[str] = set(backup["samples"])
    merged: dict[tuple[str, int], dict] = {}
    conflicts: list[dict] = []
    stats = {
        "from_backup_only": 0,
        "from_province_only": 0,
        "matched": 0,
        "acked": 0,
        "pending": 0,
        "conflicts": 0,
    }

    now = _now()
    for key, r in backup_rows.items():
        merged[key] = {
            "sample_no": key[0],
            "revision_no": key[1],
            "concentration": r["concentration"],
            "unit": r.get("unit") or "ug/L",
            "handoff_id": r["handoff_id"],
            "status": "acked" if r.get("status") == "acked" else "pending",
            "attempts": int(r.get("attempts") or 0),
            "receipt_json": r.get("receipt_json"),
            "created_at": _epoch(r.get("created_at")) or now,
            "last_sent_at": _epoch(r.get("last_sent_at")),
            "acked_at": _epoch(r.get("acked_at")),
            "source": "backup",
        }

    for p in province_rows:
        sample = p["sample_no"]
        rev = p["revision_no"]
        conc = p["concentration"]
        if not isinstance(rev, int) or isinstance(rev, bool) or rev < 1:
            raise RecoveryError(f"省库记录含非法修订号: {sample}/{rev!r}")
        if not isinstance(conc, int) or isinstance(conc, bool) or conc < 0:
            raise RecoveryError(f"省库记录含非法浓度: {sample}/r{rev}={conc!r}")
        key = (sample, rev)
        samples.add(sample)
        receipt = build_receipt(p)
        receipt_json = json.dumps(receipt, ensure_ascii=False)
        if key in merged:
            cur = merged[key]
            if cur["handoff_id"] != p["handoff_id"]:
                conflicts.append({
                    "sample_no": sample, "revision_no": rev,
                    "field": "handoff_id",
                    "backup": cur["handoff_id"], "province": p["handoff_id"],
                })
                continue
            if cur["concentration"] != conc:
                conflicts.append({
                    "sample_no": sample, "revision_no": rev,
                    "field": "concentration",
                    "backup": cur["concentration"], "province": conc,
                })
                continue
            cur["status"] = "acked"
            cur["receipt_json"] = receipt_json
            cur["acked_at"] = _epoch(p["received_at"]) or now
            cur["source"] = "backup+province"
        else:
            merged[key] = {
                "sample_no": sample,
                "revision_no": rev,
                "concentration": conc,
                "unit": p.get("unit") or "ug/L",
                "handoff_id": p["handoff_id"],
                "status": "acked",
                "attempts": 1,
                "receipt_json": receipt_json,
                "created_at": _epoch(p["received_at"]) or now,
                "last_sent_at": _epoch(p["received_at"]),
                "acked_at": _epoch(p["received_at"]) or now,
                "source": "province",
            }

    rows = [merged[k] for k in sorted(merged)]
    # 同一交接号不能指向两个不同修订(否则提交时会撞 handoff_id UNIQUE)
    by_handoff: dict[str, tuple[str, int]] = {}
    for r in rows:
        prev = by_handoff.get(r["handoff_id"])
        if prev is not None and prev != (r["sample_no"], r["revision_no"]):
            conflicts.append({
                "sample_no": r["sample_no"], "revision_no": r["revision_no"],
                "field": "handoff_id",
                "backup": r["handoff_id"], "province": None,
                "other": {"sample_no": prev[0], "revision_no": prev[1]},
            })
        by_handoff[r["handoff_id"]] = (r["sample_no"], r["revision_no"])

    for r in rows:
        if r["source"] == "backup":
            stats["from_backup_only"] += 1
        elif r["source"] == "province":
            stats["from_province_only"] += 1
        else:
            stats["matched"] += 1
        if r["status"] == "acked":
            stats["acked"] += 1
        else:
            stats["pending"] += 1
    stats["conflicts"] = len(conflicts)

    result = dict(stats)
    result["revision_count"] = len(rows)
    result["sample_count"] = len(samples)
    result["station_code"] = station_code
    result["source_backup"] = source_backup
    result["recovered_at"] = datetime.fromtimestamp(now, tz=timezone.utc).isoformat()
    return {"rows": rows, "conflicts": conflicts, "stats": result, "samples": samples}
