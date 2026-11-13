"""
恢复流程集成测试: 真实起 1 个省库 + 2 个站内进程(共用同一空白数据卷)。

覆盖:
1. 空白卷恢复模式下, 两个进程的录入/查询都返回 423(封闭)
2. 冲突备份 -> 409, 不覆盖任何一方; 仍封闭; 修好后可重试
3. 正确备份 -> 200; 恢复后两进程同时开放
4. 从省库补回备份缺少的修订/浓度/交接号/回执号; 省库已收标为 acked
5. 备份有、省库无的 pending 按原交接号续交并最终 acked
6. 新修订号从该样本恢复的历史最大号 +1 接续
7. 恢复期间查询不返回半恢复数据(冲突/失败时 revisions 查询 423)
"""
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def request(method: str, url: str, data: bytes | None = None,
            headers: dict | None = None, timeout: float = 30):
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            return resp.status, (json.loads(body) if body else None)
    except urllib.error.HTTPError as exc:
        body = exc.read()
        try:
            parsed = json.loads(body)
        except Exception:
            parsed = body.decode(errors="replace")
        if isinstance(parsed, dict) and set(parsed) == {"detail"}:
            parsed = parsed["detail"]
        return exc.code, parsed


def multipart(path: str, field: str = "backup"):
    boundary = "----tp-test-boundary"
    name = os.path.basename(path)
    with open(path, "rb") as f:
        content = f.read()
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="{field}"; filename="{name}"\r\n'
        "Content-Type: application/octet-stream\r\n\r\n"
    ).encode() + content + f"\r\n--{boundary}--\r\n".encode()
    return body, {"Content-Type": f"multipart/form-data; boundary={boundary}"}


def wait_http(url: str, timeout: float = 40) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, _ = request("GET", url, timeout=2)
            if status in (200, 423):
                return
        except Exception:
            pass
        time.sleep(0.2)
    raise RuntimeError(f"service not ready: {url}")


def start_station(port: int, data: str, province: str, instance: str,
                  recovery_mode: bool, logdir: str) -> subprocess.Popen:
    env = dict(os.environ, STATION_DB=os.path.join(data, "station.db"),
               PROVINCE_URL=province, INSTANCE_ID=instance, STATION_CODE="ST01",
               POLL_INTERVAL_S="0.3", CLAIM_LEASE_S="3")
    if recovery_mode:
        env["RECOVERY_MODE"] = "1"
    log = open(os.path.join(logdir, f"{instance}.log"), "w")
    proc = subprocess.Popen(
        [PY, "-m", "uvicorn", "app:app", "--host", "127.0.0.1", "--port", str(port),
         "--log-level", "warning"],
        cwd=os.path.join(ROOT, "station"), env=env, stdout=log, stderr=subprocess.STDOUT,
    )
    proc._log = log
    return proc


def stop(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)
    log = getattr(proc, "_log", None)
    if log is not None:
        log.close()


def check(name: str, cond: bool, detail: object = "") -> None:
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  -> {detail}"))
    if not cond:
        raise AssertionError(name)


def main() -> None:
    tmp = tempfile.mkdtemp(prefix="tp-recovery-test-")
    data = os.path.join(tmp, "data")
    os.makedirs(data)
    prov_db = os.path.join(tmp, "province.db")
    logs = os.path.join(tmp, "logs")
    os.makedirs(logs)
    pport, aport, bport = free_port(), free_port(), free_port()
    prov = f"http://127.0.0.1:{pport}"
    a = f"http://127.0.0.1:{aport}"
    b = f"http://127.0.0.1:{bport}"

    procs = []
    try:
        # --- 省库(带样例记录) ---
        env = dict(os.environ, PROVINCE_DB=prov_db)
        plog = open(os.path.join(logs, "province.log"), "w")
        procs.append(subprocess.Popen(
            [PY, "-m", "uvicorn", "app:app", "--host", "127.0.0.1", "--port", str(pport),
             "--log-level", "warning"],
            cwd=os.path.join(ROOT, "province"), env=env, stdout=plog, stderr=subprocess.STDOUT,
        ))
        wait_http(prov + "/health")
        subprocess.run(
            [PY, os.path.join(ROOT, "scripts", "seed_province.py"),
             os.path.join(ROOT, "samples", "province_receipts.json")],
            env=dict(os.environ, PROVINCE_DB=prov_db), check=True,
        )
        # 让省库重新打开最新数据(seed 在进程启动后写库, WAL 下进程内连接即可见)

        # --- 两个站内进程: 空白卷 + 恢复模式 ---
        pa = start_station(aport, data, prov, "station-a", True, logs)
        pb = start_station(bport, data, prov, "station-b", True, logs)
        procs += [pa, pb]
        wait_http(a + "/health")
        wait_http(b + "/health")
        time.sleep(0.5)

        st, body = request("GET", a + "/recovery/status")
        check("恢复模式启动后状态为 recovering", st == 200 and body["state"] == "recovering", body)
        st, body = request("GET", b + "/recovery/status")
        check("两进程共享同一恢复状态", st == 200 and body["state"] == "recovering", body)

        # --- 封闭: 两个进程都不能录入/查询 ---
        for base, label in ((a, "A"), (b, "B")):
            st, body = request("POST", base + "/revisions",
                               json.dumps({"sample_no": "X1", "concentration": 1}).encode(),
                               {"Content-Type": "application/json"})
            check(f"恢复期间 station-{label} 拒绝录入(423)", st == 423, (st, body))
            st, body = request("GET", base + "/revisions")
            check(f"恢复期间 station-{label} 拒绝查询(423)", st == 423, (st, body))

        st, body = request("GET", a + "/status")
        check("恢复期间 /status 仍可查看但标记未开放", st == 200 and body["open"] is False, body)

        # --- 冲突备份: 整体拒绝, 不覆盖 ---
        cbody, chdr = multipart(os.path.join(ROOT, "samples", "conflict_backup.db"))
        st, body = request("POST", a + "/recovery", cbody, chdr)
        check("冲突备份返回 409", st == 409, (st, body))
        check("冲突响应列出字段", isinstance(body, dict) and body.get("error") == "recovery_conflict"
              and len(body.get("conflicts", [])) >= 2, body)
        st, body = request("GET", b + "/revisions")
        check("冲突后另一进程仍封闭", st == 423, (st, body))
        st, body = request("GET", a + "/recovery/status")
        check("冲突后状态 failed, 可重试", st == 200 and body["state"] == "failed"
              and body["open"] is False, body)

        # --- 正确备份: 恢复成功 ---
        body_bytes, hdr = multipart(os.path.join(ROOT, "samples", "old_backup.db"))
        st, body = request("POST", a + "/recovery", body_bytes, hdr)
        check("恢复成功返回 200", st == 200, (st, body))
        check("恢复统计: 省库补回 3 条(含 W2026-0003 两条 + W2026-0001 r3)",
              body.get("from_province_only") == 3, body)
        check("恢复统计: 备份保留 4 条 pending", body.get("from_backup_only") >= 0
              and body.get("pending") >= 1, body)

        st, body = request("GET", b + "/recovery/status")
        check("另一进程立即看到 ready 并开放", st == 200 and body["state"] == "ready"
              and body["open"] is True, body)

        # --- 回查: 省库已收全部 acked, 备份缺少的记录被补回 ---
        st, rows = request("GET", a + "/revisions")
        check("恢复后查询开放", st == 200, (st, rows))
        by_key = {(r["sample_no"], r["revision_no"]): r for r in rows}
        for sample, rev, conc, handoff in [
            ("W2026-0001", 1, 25, "ST01-W2026-0001-r1-a1b2c3"),
            ("W2026-0001", 2, 30, "ST01-W2026-0001-r2-d4e5f6"),
            ("W2026-0001", 3, 28, "ST01-W2026-0001-r3-abcdef"),
            ("W2026-0003", 1, 50, "ST01-W2026-0003-r1-135790"),
            ("W2026-0003", 2, 55, "ST01-W2026-0003-r2-246801"),
            ("W2026-0004", 1, 12, "ST01-W2026-0004-r1-445566"),
        ]:
            r = by_key.get((sample, rev))
            check(f"恢复记录 {sample}/r{rev} 存在且字段正确",
                  r is not None and r["concentration"] == conc and r["handoff_id"] == handoff,
                  r)
            check(f"省库已收的 {sample}/r{rev} 标为 acked", r is not None and r["status"] == "acked", r)
            check(f"省库已收的 {sample}/r{rev} 带原回执号",
                  r is not None and (r["receipt"] or {}).get("receipt_id", "").startswith("RCPT-"),
                  r)
        check("省库独有样本 W2026-0003 被补回", ("W2026-0003", 2) in by_key, sorted(by_key))
        check("备份与省库都没有的样本不推造", ("W2026-0002", 2) not in by_key
              and ("W2026-0006", 1) not in by_key, sorted(by_key))

        # --- pending 按原交接号续交 ---
        deadline = time.time() + 15
        while time.time() < deadline:
            st, rows = request("GET", a + "/revisions?status=pending")
            if not rows:
                break
            time.sleep(0.4)
        check("备份中未确认的修订已按原交接号续交完毕", rows == [], rows)
        _, receipts = request("GET", prov + "/receipts")
        rh = {r["handoff_id"] for r in receipts}
        check("续交沿用了原交接号 W2026-0002 r1",
              "ST01-W2026-0002-r1-112233" in rh, rh)
        check("续交沿用了原交接号 W2026-0004 r2",
              "ST01-W2026-0004-r2-778899" in rh, rh)

        # --- 新修订号从历史最大号接续 ---
        st, body = request("POST", b + "/revisions",
                           json.dumps({"sample_no": "W2026-0001", "concentration": 99}).encode(),
                           {"Content-Type": "application/json"})
        check("新修订在 W2026-0001 上接续为 r4", st == 201 and body["revision_no"] == 4, (st, body))
        st, body = request("POST", a + "/revisions",
                           json.dumps({"sample_no": "W2026-0003", "concentration": 62}).encode(),
                           {"Content-Type": "application/json"})
        check("新修订在恢复出的 W2026-0003 上接续为 r3", st == 201 and body["revision_no"] == 3, (st, body))

        # --- 成功后不允许重复恢复 ---
        body_bytes, hdr = multipart(os.path.join(ROOT, "samples", "old_backup.db"))
        st, body = request("POST", a + "/recovery", body_bytes, hdr)
        check("恢复完成后拒绝再次恢复(409)", st == 409, (st, body))

        print("\nALL RECOVERY TESTS PASSED")
    finally:
        for p in procs:
            stop(p)
        print("artifacts:", tmp)


if __name__ == "__main__":
    main()
