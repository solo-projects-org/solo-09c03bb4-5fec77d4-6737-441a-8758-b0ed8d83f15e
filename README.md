# 总磷交接系统(站内 FastAPI + SQLite ↔ 省库模拟)

水质站数据员把 LIMS 审核后的总磷结果交接给省级监测数仓:断网时先留站内,恢复后自动补齐。

## 架构

```
                ┌────────────────┐   共用 SQLite (station-data 卷)
 录入/查询 ───▶ │ station-a :8001 │──┐
 (Swagger)      └────────────────┘  │  租约(claim)协调, 共同交接
                ┌────────────────┐  │        │ 重试/续交
 录入/查询 ───▶ │ station-b :8002 │──┘        ▼
 (Swagger)      └────────────────┘   ┌──────────────────┐
                                     │ province :8000   │  本地省库模拟
                                     │ 按交接号持久去重  │  可注入: 丢回执/慢响应
                                     └──────────────────┘
```

- **station**(两个进程):录入修订、生成修订号与固定交接号、后台发送器持续重试、重启续交。
- **province**(本地省库):按交接号持久去重,重复报送返回原回执;可模拟"收妥后丢回执"和慢响应;可查看接收顺序。

## 快速启动

```bash
docker compose up --build
```

| 服务 | 地址 | Swagger |
|---|---|---|
| station-a | http://localhost:8001 | http://localhost:8001/docs |
| station-b | http://localhost:8002 | http://localhost:8002/docs |
| province | http://localhost:8000 | http://localhost:8000/docs |

一键演示(含顺序性、丢回执恢复、慢响应并行):

```bash
bash scripts/demo.sh
```

## 需求 → 实现对照

| 需求 | 实现 |
|---|---|
| 提交样本号 + 非负整数浓度(μg/L) | `POST /revisions`,pydantic 校验 `concentration >= 0` 的整数 |
| 每次生成修订号和固定交接号 | 样本内递增 `revision_no`(事务内分配);`handoff_id` 落库后固定不变 |
| 查各修订及交接状态 | `GET /revisions`、`GET /revisions/{handoff_id}`、`GET /samples/{no}/revisions`、`GET /status` |
| 只有本地保存成功的修订才能发送 | 发送器只扫描 SQLite 中已提交的行 |
| 未获收妥确认的持续重试 | 后台发送器指数退避重试(1s→15s 封顶) |
| 重启后续交 | 启动时扫描全部 `pending` 修订;租约过期后被任一进程接管 |
| 省库按交接号持久去重 | `receipts.handoff_id UNIQUE`,落库于 SQLite 卷 |
| 重复报送返回原回执 | 冲突时查出首次记录,返回原 `receipt_id` 且 `duplicate=true` |
| 两个服务进程共同交接 | 两进程共用同一 SQLite 文件,原子 `UPDATE ... WHERE claim 过期` 认领 |
| 同一样本按修订顺序发送 | 只有"最早的未确认修订"(头修订)可进入发送队列 |
| 不同样本并行,慢响应不卡住其他样本或本地录入 | 每头修订一个异步任务(信号量限流 50),httpx 异步发送;录入只做快速本地写 |
| 可模拟收妥后丢回执 | 省库 `POST /admin/config {"mode":"drop_receipt"}` 或 `{"drop_next":N}` |
| 查看省库接收顺序 | `GET /receipts`(按 `received_seq` 排序) |

## 主要 API

**station(8001/8002)**

- `POST /revisions` — 录入 `{ "sample_no": "W2026-0001", "concentration": 25 }` → 201,返回修订号、交接号、状态
- `GET /revisions?sample_no=&status=` — 各修订及交接状态(`pending`/`acked`、尝试次数、回执)
- `GET /revisions/{handoff_id}` — 按交接号查询
- `GET /samples/{sample_no}/revisions` — 某样本全部修订(按修订号)
- `GET /status` — 实例与交接概况

**province(8000)**

- `POST /handoff` — 接收交接(站内调用);重复交接号返回原回执
- `GET /receipts` — 接收顺序
- `GET/POST /admin/config` — 故障注入:`mode`(`normal`/`drop_receipt`)、`delay_ms`、`drop_next`
- `POST /admin/reset` — 清空接收记录

## 场景验证

**断网留站内,恢复补齐**

```bash
docker compose stop province
curl -X POST localhost:8001/revisions -H 'Content-Type: application/json' \
     -d '{"sample_no":"S9","concentration":12}'     # 保持 pending, 持续重试
docker compose start province                      # 数秒内自动补齐为 acked
```

**重启续交 / 双进程接管**

```bash
docker compose stop province
curl -X POST localhost:8001/revisions -H 'Content-Type: application/json' \
     -d '{"sample_no":"S8","concentration":9}'
docker compose kill station-a                      # 杀掉一个进程
docker compose start province                      # station-b 租约接管并完成交接
docker compose start station-a                     # 重启后自动扫描 pending 续交
```

**收妥后丢回执 → 原回执**

```bash
curl -X POST localhost:8000/admin/config -H 'Content-Type: application/json' -d '{"drop_next":1}'
curl -X POST localhost:8001/revisions -H 'Content-Type: application/json' \
     -d '{"sample_no":"S7","concentration":33}'    # 第一次发送拿到 500, 持续 pending
curl localhost:8000/receipts                        # 省库其实已收到
# 站内重试时省库按交接号去重, 返回首次的原回执(duplicate=true), 修订转为 acked
```

**慢响应不阻塞**:省库 `{"delay_ms":3000}` 时,不同样本的 `acked_at` 几乎相同(并行),且录入接口毫秒级返回。

## 环境变量

**station**:`STATION_DB`(默认 `station.db`)、`PROVINCE_URL`、`INSTANCE_ID`、`STATION_CODE`、
`SEND_TIMEOUT_S`(5)、`POLL_INTERVAL_S`(1)、`CLAIM_LEASE_S`(15)、`MAX_INFLIGHT`(50)

**province**:`PROVINCE_DB`、`PROVINCE_MODE`、`PROVINCE_DELAY_MS`

## 可靠性说明

- 交接号在修订落库时生成并固定,重试/重启不变 —— 这是省库去重键。
- 认领租约(默认 15s)防止两进程同时发送同一修订;进程崩溃后租约过期,由另一进程接管。
- 即便极端情况下同一修订被发送两次(如租约过期瞬间),省库去重仍返回原回执,端到端幂等。
- 站内先落库后发送;省库先落库后回执 —— 任一环节崩溃,恢复后都能收敛到一致状态。
