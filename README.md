# 总磷交接系统(站内 FastAPI + SQLite ↔ 省库模拟)

水质站数据员把 LIMS 审核后的总磷结果交接给省级监测数仓:断网时先留站内,恢复后自动补齐。
站内盘损坏时,可用旧备份 + 省库已收记录重建交接进度(见"恢复"一节)。

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

## 恢复:站内盘损坏后重建交接进度

**适用场景**:站内 SQLite 卷损坏,手上还有一份旧备份,省库记录完好。
恢复在**空白站内卷**上进行,用恢复模式启动两个站内进程,上传同版、同站的备份;
服务从省库读取已收记录,合并补回备份缺少的修订、浓度、交接号和回执号。

```bash
docker compose down -v                                   # 丢弃损坏的站内卷
docker compose up -d province                             # 省库(恢复期间不得有其他写入)
python3 scripts/seed_province.py samples/province_receipts.json --via-api  # 演示用省库样例
docker compose --profile recovery up -d --build station-a-recovery station-b-recovery
```

然后任选一个进程上传备份(也可直接在 Swagger 里操作):

```bash
# 冲突备份 -> 409, 不覆盖任何一方
curl -X POST localhost:8001/recovery -F 'backup=@samples/conflict_backup.db'

# 旧备份 -> 200, 恢复完成, 两个进程同时开放
curl -X POST localhost:8001/recovery -F 'backup=@samples/old_backup.db'
curl localhost:8001/recovery/status
curl localhost:8001/revisions
curl localhost:8001/samples/W2026-0003/revisions
```

完整逐步演示(含 Swagger 回查与恢复后继续交接):

```bash
bash scripts/demo_recovery.sh
```

样例文件:

| 文件 | 用途 |
|---|---|
| `samples/old_backup.db` | 旧站内备份:`W2026-0001`/`0002`/`0004`/`0005`,部分 pending |
| `samples/conflict_backup.db` | 与省库样例存在交接号/浓度冲突的备份(用于验证拒绝) |
| `samples/province_receipts.json` | 省库样例:含备份缺少的 `W2026-0001 r3`、整样本 `W2026-0003` |
| `samples/expected_after_recovery.json` | 恢复后应看到的状态(便于核对) |

### 合并语义

| 备份 | 省库 | 恢复后 |
|---|---|---|
| 有,已确认 | 有 | 保持已确认,沿用备份交接号与回执 |
| 有,未确认 | 有 | 标为已确认,补上省库回执号 |
| 有,未确认 | 无 | 保持未确认,按**原交接号**续交 |
| 无 | 有 | 补回修订、浓度、交接号、回执号,标为已确认 |
| 无 | 无 | **不推造** |

- 同一样本同一修订号的**交接号或浓度不一致**→ 整个恢复拒绝(409),冲突明细随响应返回,不覆盖任何一方。
- 恢复只在空白卷、`RECOVERY_MODE=1`、且当前无修订时执行;已完成后不允许再次恢复。

### 一致性与封闭

- 恢复状态持久在共享 SQLite 的 `recovery_state` 表中,**两个站内进程用同一份状态**:
  录入、发送、查询在恢复期间都返回 `423`;`/status`、`/recovery/status` 仍可查看进度。
- 合并结果在**单个 SQLite 写事务**内提交:查询要么看不到任何恢复数据,要么看到完整结果,
  不会返回半恢复的修订。恢复期间省库无其他写入,读取的是一次完整快照。
- 备份校验(`integrity_check`、表结构、站点码、修订号/浓度合法性)失败、省库读取失败、
  冲突、进程中途退出:都保持封闭,状态记为 `failed`/`recovering`,**重启后可重试**
  (`POST /recovery` 重新上传,或 `POST /recovery/retry` 清退避后继续发送)。
- 恢复成功后,各样本的新修订号从该样本恢复出的历史最大号 `+1` 接续。

### 恢复相关 API

**station(8001/8002)**

- `POST /recovery` — multipart 上传备份(`backup=@old_backup.db`);成功返回合并统计,
  失败返回 `409`(冲突/备份非法/已恢复)或 `502`(读取备份或省库失败),均保持封闭
- `GET /recovery/status` — `state`(`closed`/`recovering`/`ready`/`failed`)、是否开放、
  上次失败原因与冲突明细
- `POST /recovery/retry` — 失败后清退避、恢复发送器(不重新读取备份)
- `GET /status` — 增加 `open` 与 `recovery` 字段

**恢复模式配置**:`RECOVERY_MODE=1`(必需)、`RECOVERY_TIMEOUT_S`(30,读取省库超时)、
`RECOVERY_ATTEMPT_LEASE_S`(20,进程中途退出后另一进程可接管/重试的等待)、
`MAX_BACKUP_BYTES`(512MB)。

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
| 空白卷恢复模式上传同版同站备份 | `POST /recovery`(multipart),`RECOVERY_MODE=1` |
| 从省库补回修订/浓度/交接号/回执号 | 恢复时 `GET /receipts` 后与备份按 `(样本, 修订号)` 合并 |
| 省库已收标为已确认,其余按原交接号续交 | 合并时置 `acked`;未收的保留原 `handoff_id` 进入发送队列 |
| 同修订号交接号/浓度不一致报冲突不覆盖 | 合并前逐项比对,冲突则整体 409,不写任何行 |
| 恢复期间省库无其他写入 | 恢复只读取一次省库快照,不做写入 |
| 两个进程等恢复完整生效才开放 | `recovery_state` 持久在共享库,录入/发送/查询逐个事务校验 `ready` |
| 读写失败/中途退出保持封闭,重启可重试 | 状态置 `failed`/保留 `recovering`;重启后 `POST /recovery` 重试 |
| 查询不返回半恢复的修订 | 合并行在单个写事务提交;查询在开放前一律 423 |
| 新修订号从恢复的历史最大号接续 | 沿用 `MAX(revision_no)+1` 的样本内分配 |
| 备份和省库都没有的不推造 | 合并只遍历两份来源的并集 |
| 可启动 Compose、样例、Swagger 回查/续交 | `docker-compose.yml` 的 `recovery` profile + `samples/` + `scripts/demo_recovery.sh` |

## 主要 API

**station(8001/8002)**

- `POST /revisions` — 录入 `{ "sample_no": "W2026-0001", "concentration": 25 }` → 201,返回修订号、交接号、状态
- `GET /revisions?sample_no=&status=` — 各修订及交接状态(`pending`/`acked`、尝试次数、回执)
- `GET /revisions/{handoff_id}` — 按交接号查询
- `GET /samples/{sample_no}/revisions` — 某样本全部修订(按修订号)
- `GET /status` — 实例、交接与恢复概况
- `POST /recovery`、`GET /recovery/status`、`POST /recovery/retry` — 见"恢复"一节

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

**本地验证恢复(无需 Docker)**

```bash
bash scripts/verify_recovery.sh   # 生成样例, 起真实进程跑 e2e + 边界测试
```

## 环境变量

**station**:`STATION_DB`(默认 `station.db`)、`PROVINCE_URL`、`INSTANCE_ID`、`STATION_CODE`、
`SEND_TIMEOUT_S`(5)、`POLL_INTERVAL_S`(1)、`CLAIM_LEASE_S`(15)、`MAX_INFLIGHT`(50)、
`RECOVERY_MODE`(0)、`RECOVERY_TIMEOUT_S`(30)、`RECOVERY_ATTEMPT_LEASE_S`(20)、`MAX_BACKUP_BYTES`

**province**:`PROVINCE_DB`、`PROVINCE_MODE`、`PROVINCE_DELAY_MS`

## 可靠性说明

- 交接号在修订落库时生成并固定,重试/重启不变 —— 这是省库去重键。
- 认领租约(默认 15s)防止两进程同时发送同一修订;进程崩溃后租约过期,由另一进程接管。
- 即便极端情况下同一修订被发送两次(如租约过期瞬间),省库去重仍返回原回执,端到端幂等。
- 站内先落库后发送;省库先落库后回执 —— 任一环节崩溃,恢复后都能收敛到一致状态。
- 恢复提交使用 `BEGIN IMMEDIATE` 单事务;`recovery_state` 与 `revisions` 同库,
  两个进程看到的开放/封闭状态始终一致。

