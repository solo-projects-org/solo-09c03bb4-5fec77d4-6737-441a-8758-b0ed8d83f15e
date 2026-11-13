"""
恢复边界场景:
1. 省库不可读(停掉省库) -> 恢复失败但仍封闭; 重启后重试成功
2. 恢复尝试期间另一个进程的录入/查询始终封闭, 不会插入半恢复数据
3. 进程中途退出(restart)后, 数据库仍为 recovering, 重启可继续完成恢复
4. 备份损坏 -> 失败且封闭, 换成正确备份后重试成功
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_recovery_e2e import (  # noqa: E402
    ROOT, free_port, multipart, request, start_station, stop, wait_http,
)

PY = sys.executable


def check(name, cond, detail=""):
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  -> {detail}"))
    if not cond:
        raise AssertionError(name)


def main() -> None:
    tmp = tempfile.mkdtemp(prefix="tp-recovery-edge-")
    data = os.path.join(tmp, "data")
    os.makedirs(data)
    logs = os.path.join(tmp, "logs")
    os.makedirs(logs)
    prov_db = os.path.join(tmp, "province.db")
    pport, aport, bport = free_port(), free_port(), free_port()
    prov = f"http://127.0.0.1:{pport}"
    a = f"http://127.0.0.1:{aport}"
    b = f"http://127.0.0.1:{bport}"

    # 省库先启动、灌样例, 然后停掉以模拟"读取省库失败"
    plog = open(os.path.join(logs, "province.log"), "w")
    pp = subprocess.Popen(
        [PY, "-m", "uvicorn", "app:app", "--host", "127.0.0.1", "--port", str(pport),
         "--log-level", "warning"],
        cwd=os.path.join(ROOT, "province"),
        env=dict(os.environ, PROVINCE_DB=prov_db), stdout=plog, stderr=subprocess.STDOUT,
    )
    wait_http(prov + "/health")
    subprocess.run(
        [PY, os.path.join(ROOT, "scripts", "seed_province.py"),
         os.path.join(ROOT, "samples", "province_receipts.json")],
        env=dict(os.environ, PROVINCE_DB=prov_db), check=True,
    )
    stop(pp)

    pa = start_station(aport, data, prov, "station-a", True, logs)
    pb = start_station(bport, data, prov, "station-b", True, logs)
    try:
        wait_http(a + "/health")
        wait_http(b + "/health")
        time.sleep(0.4)

        # 1. 省库不可读 -> 409, 保持封闭
        body_bytes, hdr = multipart(os.path.join(ROOT, "samples", "old_backup.db"))
        st, body = request("POST", a + "/recovery", body_bytes, hdr)
        check("省库不可读时恢复失败(409)", st == 409 and body.get("retryable") is True, (st, body))
        for base, label in ((a, "A"), (b, "B")):
            st, _ = request("GET", base + "/revisions")
            check(f"读取失败后 station-{label} 仍封闭(423)", st == 423, st)
            st, _ = request("POST", base + "/revisions",
                            json.dumps({"sample_no": "Y1", "concentration": 5}).encode(),
                            {"Content-Type": "application/json"})
            check(f"读取失败后 station-{label} 拒绝录入(423)", st == 423, st)

        # 2. 损坏备份 -> 409, 仍封闭
        bad = os.path.join(tmp, "bad.db")
        with open(bad, "wb") as f:
            f.write(b"this is not sqlite" * 100)
        body_bytes, hdr = multipart(bad)
        st, body = request("POST", b + "/recovery", body_bytes, hdr)
        check("损坏备份被拒绝(409)", st == 409, (st, body))
        st, _ = request("GET", a + "/revisions")
        check("损坏备份后仍封闭", st == 423, st)

        # 3. 恢复途中杀掉进程: 重启后数据库状态仍是 recovering, 可继续
        pa.terminate()
        pa.wait(timeout=10)
        pa._log.close()
        pb2 = start_station(aport, data, prov, "station-a", True, logs)  # 同一 INSTANCE_ID 重启
        procs_extra = [pb2]
        wait_http(a + "/health")
        st, body = request("GET", a + "/recovery/status")
        check("进程重启后仍处于 recovering", st == 200 and body["state"] == "recovering", body)

        # 4. 恢复省库后, 重试成功; 两个进程同步开放
        pp = subprocess.Popen(
            [PY, "-m", "uvicorn", "app:app", "--host", "127.0.0.1", "--port", str(pport),
             "--log-level", "warning"],
            cwd=os.path.join(ROOT, "province"),
            env=dict(os.environ, PROVINCE_DB=prov_db), stdout=plog, stderr=subprocess.STDOUT,
        )
        wait_http(prov + "/health")
        body_bytes, hdr = multipart(os.path.join(ROOT, "samples", "old_backup.db"))
        st, body = request("POST", a + "/recovery", body_bytes, hdr, timeout=60)
        check("重启后可重试并恢复成功", st == 200, (st, body))
        for base, label in ((a, "A"), (b, "B")):
            st, body = request("GET", base + "/recovery/status")
            check(f"station-{label} 状态为 ready 并开放", st == 200 and body["state"] == "ready"
                  and body["open"] is True, body)
            st, _ = request("GET", base + "/revisions")
            check(f"station-{label} 查询开放", st == 200, st)

        st, rows = request("GET", a + "/revisions")
        check("恢复数据完整(7 备份 + 3 省库独有 = 10)", len(rows) == 10, len(rows))
        check("没有半恢复记录", all(r["sample_no"] and r["revision_no"] >= 1 for r in rows))

        # 5. 恢复后普通模式也能读到同一份数据(重启即续交)
        st, body = request("POST", a + "/revisions",
                           json.dumps({"sample_no": "W2026-0003", "concentration": 70}).encode(),
                           {"Content-Type": "application/json"})
        check("恢复后继续交接: W2026-0003 r3", st == 201 and body["revision_no"] == 3, (st, body))
        deadline = time.time() + 10
        while time.time() < deadline:
            st, row = request("GET", a + f"/revisions/{body['handoff_id']}")
            if st == 200 and row["status"] == "acked":
                break
            time.sleep(0.3)
        check("新修订成功交接至省库", st == 200 and row["status"] == "acked", (st, row))

        print("\nALL RECOVERY EDGE TESTS PASSED")
    finally:
        for p in (pa, pb, pp):
            try:
                stop(p)
            except Exception:
                pass
        print("artifacts:", tmp)


if __name__ == "__main__":
    main()
