# lock-lab —— 库位调拨行锁互斥实验（Flask + PostgreSQL）

复盘两笔库位调拨互相卡住的场景。两笔样例调拨在各自独立的数据库连接/事务里
对**同两条库存记录**执行 `SELECT ... FOR UPDATE`，页面按调拨单展示 PostgreSQL
中**实际观测到**的等待关系；只有数据库自己报死锁（SQLSTATE `40P01`）才判定为死锁。
每轮结束后可在轮次详情里**冲销**一笔已确认提交的调拨，按原数量反向调回，
库存与冲销记录在同一事务中落账。

## 业务场景

| 调拨单 | 方向 | 数量 |
|---|---|---|
| TR-1001（T1） | A-01 → A-02 | 10 |
| TR-1002（T2） | A-02 → A-01 | 10 |

初始两库位各 100 件。每笔事务：`BEGIN` → 按顺序对两条记录 `FOR UPDATE` 加行锁
→ 扣减/增加库存 → `COMMIT`。

页面可切换两种加锁次序：

- **相反次序（opposite）**：T1 先 A-01 后 A-02，T2 先 A-02 后 A-01
  → 互相等待，PostgreSQL 达到 `deadlock_timeout` 后检测出等待环，回滚其中一笔，另一笔提交。
- **相同次序（same）**：两笔都按 A-01 → A-02 加锁
  → 只排队、不成环，不会死锁，两笔先后提交。

## 快速启动（Docker Compose）

```bash
cd lock-lab
docker compose up --build
# 打开 http://localhost:8000
```

- `db`：`postgres:15-alpine`，首次启动自动执行 `db/init.sql` 建 `inventory`、`reversals`
  表与 `v_lock_waits` 视图，带健康检查；`deadlock_timeout=500ms`、`log_lock_waits=on`。
  已有数据卷不会重跑 init 脚本，应用启动时会幂等补建 `reversals` 表。
- `web`：Python 3.12 + gunicorn（1 进程 8 线程，每笔调拨独占一条 psycopg2 连接）。
- 停止并清库：`docker compose down -v`。

页面上可调参数：`lock_timeout`、`deadlock_timeout`、首锁后停顿 `stagger`
（保证两笔先各持一把锁）、硬性超时秒数。

## 实现要点

### 1. 等待关系只认数据库地面真值

`db/init.sql` 中的 `v_lock_waits` 视图以 `pg_blocking_pids()` 为等待关系来源，
关联 `pg_stat_activity` 取双方应用名、语句与状态。

> 注意一个 PG15 的实际行为：行级锁等待者在 `pg_locks` 里只留下未授予的
> `transactionid / ShareLock`，**没有 tuple 条目**，因此经典的 pg_locks
> 自连接写法能看到等待边但反查不到具体库位。本项目用 `pg_blocking_pids()`
> 保证等待边不丢；库位信息优先取视图中的 tuple 反查，缺失时由应用侧
> 根据“该连接当前正在请求的库位”补注（边数据上带 `location_source` 字段区分来源）。

### 2. 并发编排与采样

- 每轮由 supervisor 线程启动 T1、T2 两个工作线程 + 一个监控线程，
  三者在首锁请求前通过 `threading.Barrier(3)` 同时起跑，保证从第一刻起就在采样。
- 监控线程每 **100ms** 查询一次 `v_lock_waits`，全部快照随轮次保留，
  页面展示每条等待边（谁等谁、等哪个库位、锁类型、等待时长、等待中的 SQL）。
- 等待环只用于页面高亮：把所有采样到的边构图后 DFS 找环；
  **死锁判定唯一依据是数据库返回的 40P01**，应用不自行推断。

### 3. 结局与超时

- 工作线程捕获异常后按 SQLSTATE 分类：`40P01` 死锁回滚 / `55P03` lock_timeout 回滚 / 其他回滚，
  成功则提交；最终每笔调拨单显示实际结局与说明。
- **lock_timeout**：会话级 `SET lock_timeout`，超时该笔回滚（55P03），页面显示为“回滚但非死锁”。
- **硬性超时**：监控线程到点设置取消信号并 `pg_terminate_backend()` 本轮两个后端，
  连接在 worker 的 `finally` 中也会 `close()`，确保本轮连接全部释放，之后可立刻重新试跑。
- **手动取消**：`POST /api/runs/<id>/cancel`，对阻塞中的后端调用 `pg_cancel_backend()`。
- 每轮开始前把库存重置为 100/100；轮次历史保留最近 50 轮，解锁/结束后仍可回看
  当轮等待环、被回滚的一笔、另一笔的提交结果和库存实况。

### 4. 冲销（把已确认的调拨按原数量反向调回）

值班员发现某笔调拨做错时，在轮次详情里选一笔**已确认提交**的调拨单，按原数量反向调回：

- **入口**：页面 ② 本轮结果里的“冲销”面板；接口 `POST /api/runs/<id>/reversals`
  （body：`{"transfer_key": "T1"}`）。只列出 `committed` 且尚未冲销的调拨单；
  被回滚的一笔、运行中的一轮都不可选。
- **保留原结局**：冲销不修改 worker 的原调拨结局（仍显示 `committed` 与原始说明），
  另起一张冲销卡片展示冲销后**两个库位的余量**（`from_qty_after` / `to_qty_after`），
  同时刷新页面 ④ 的库存实况。
- **只入账一次**：`reversals` 表有 `UNIQUE(run_id, transfer_key)`。事务先
  `INSERT ... ON CONFLICT DO NOTHING RETURNING`，抢到行的那个事务才反向更新库存；
  并发的重复请求会在唯一键上等待，随后读到已提交的原冲销记录并原样返回。
  首次入账返回 `201`，重提/并发重复提交返回 `200` + 原 `reversal_id` 与余量，
  库存不会二次增减。
- **同一事务**：冲销记录插入、两个库位的反向更新、冲销后余量回填在**同一个事务**里，
  任一写入失败（如触发器/约束报错）整体回滚，库存与冲销记录都不会留下半截。
- **受理规则**：
  - 只有**最近受理的一轮**可以接受新冲销；新一轮一受理，旧轮的“新冲销”立即被拒（`409`）。
    旧轮**已落账**的冲销重提仍返回原结果（幂等，不会因换轮而丢失回放语义）。
  - 该轮必须**已结束**（非 `running`），否则 `409`。
  - 已受理的冲销会占用一个 in-flight 名额；下一轮的 `reset_inventory()` 必须等它归零
    （即冲销事务真正提交/回滚、连接关闭）后才执行，避免重置与冲销互相覆盖。
  - “冲销受理”和“新一轮受理”共用同一把条件锁，谁先拿到锁谁先受理：
    冲销先受理 → 新一轮重置排在它之后；新一轮先受理 → 旧轮的新冲销直接被拒。

### HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 实验页面 |
| POST | `/api/runs` | 发起一轮（body：`lock_order` 等参数） |
| GET | `/api/runs/<id>` | 轮次详情（快照、结局、库存、等待环、已落账冲销） |
| POST | `/api/runs/<id>/reversals` | 冲销一笔已提交调拨（body：`transfer_key`）；201 首次入账 / 200 重提回放 / 409 拒绝 |
| POST | `/api/runs/<id>/cancel` | 手动取消并释放连接 |
| GET | `/api/runs` | 历史轮次列表 |
| GET | `/api/health` | 数据库连通性 |

`reversals` 表在 `db/init.sql` 中创建；应用启动时还会幂等执行一次同样的 DDL，
因此**已有数据卷**（官方镜像只在空目录执行 init 脚本）无需 `down -v` 也能升级。

## 实测记录（PostgreSQL 15.20）

以下为本地真实试跑结果（每 100ms 采样）：

### 相反次序 —— 数据库判定死锁

- `db_declared_deadlock=true`，其中一笔收到 `40P01` 被数据库回滚，另一笔正常提交。
  （当轮记录里牺牲品是 T1；具体回滚哪一笔由 PostgreSQL 死锁检测器选择，多轮试跑中也观察到过 T2 被回滚，页面按实际结果展示。）
- 采样到完整等待环（连续约 1s 的双向边，与 1s 的 `deadlock_timeout` 吻合）：
  - `locklab-T1 → locklab-T2`，等待库位 **A-02**（T1 已持 A-01）
  - `locklab-T2 → locklab-T1`，等待库位 **A-01**（T2 已持 A-02）
- 库存只体现 T2 的调拨：A-01=110、A-02=90（T1 回滚不留痕迹）。

### 相同次序 —— 普通等待，无死锁

- `db_declared_deadlock=false`，两笔均 `committed`，无等待环。
- 采样到单边排队：`locklab-T1 → locklab-T2 @ A-01`，持续约 1s 后 T2 提交放行，T1 继续。
- 两笔对冲后库存回到 100/100。

### 异常路径

- **lock_timeout 路径**：`lock_timeout=800ms` 且把 `deadlock_timeout` 调到 30s，
  两笔均收到 `55P03` 回滚，页面明确显示“回滚但数据库未判定为死锁”，库存不变。
- **硬性超时**：`hard_timeout_s=5` 到点后轮次置为 `cancelled`，
  `pg_terminate_backend` 生效，`pg_stat_activity` 中 `locklab-*` 连接数为 0，
  随后可立即发起新一轮并正常完成。
- **手动取消**：取消接口返回后两笔回滚、连接数归 0。

### 冲销实测（PostgreSQL 15.20，2026-11-22）

同一套页面/接口在 PostgreSQL 15.20 + gunicorn（1 worker / 8 threads）下用浏览器
（Playwright Chromium）与 HTTP 客户端逐项验证：

| 场景 | 结果 |
|---|---|
| 相同次序一轮（两笔 committed，100/100）后冲销 T1 | `201`；库存 A-01=110、A-02=90；原调拨卡片仍显示 `committed`；冲销卡片显示 110/90 |
| 同一条冲销顺序重提 | `200 duplicate=true`，返回同一 `reversal_id`，库存不变 |
| 6~10 路并发冲销同一单 | 恰好 1 个 `201`、其余 `200 duplicate=true`，全部返回同一 `reversal_id`，库存只反向一次 |
| 同一轮并发冲销 T1、T2 | 两条都 `201`，最终回到 100/100，`reversals` 两行 |
| 冲销触发器注入失败 | 接口 `500`；库存仍是 100/100、`reversals` 0 行（同一事务整体回滚）；去掉触发器后重试 `201` |
| 运行中的一轮请求冲销 | `409 该轮尚未结束，暂不能冲销` |
| 冲销相反次序轮里被 40P01 回滚的一笔 | `409 调拨单 T2 在本轮未确认提交（rolled_back），不能冲销` |
| 下一轮先受理后，旧轮发起新冲销 | `409 只允许冲销最近受理的一轮…`；旧轮库存不变 |
| 冲销先受理并阻塞在行锁（外部会话持锁 9s），随后再受理新一轮 | 新一轮 `POST /api/runs` 返回 `201`，但其库存重置等待冲销事务：日志出现 `inventory reset waits for 1 in-flight reversal(s)`；冲销提交后新一轮从 100/100 开始，最终 100/100 |

## 目录结构

```
lock-lab/
├── docker-compose.yml        # db(healthcheck) + web
├── README.md
├── db/
│   └── init.sql              # inventory 表 + reversals 表 + v_lock_waits 观测视图
└── app/
    ├── Dockerfile
    ├── requirements.txt
    ├── app.py                # 编排、工作线程、监控、超时、冲销、HTTP API
    ├── wait_snapshot.py      # 视图只读查询
    └── templates/index.html  # 单页前端（含冲销面板）
```

## 余项 / 已知边界

1. 交付环境未安装 Docker（无 `docker` 二进制，且不允许 unprivileged user namespace），
   实测是在本机部署的 **PostgreSQL 15.20**（与 Compose 中 `postgres:15-alpine`
   同主版本）+ 原生 Flask/gunicorn 完成；`docker-compose.yml` 仅做 YAML 与结构校验，
   **未在本机实际 `docker compose up`**。
2. 等待关系采样为 100ms 周期快照，极短于 100ms 的等待窗口可能采不到；
   默认 `deadlock_timeout=1000ms` 正是为了让等待环稳定落入采样窗口。
3. 为便于复盘，全局同一时间只允许一轮实验在跑（第二个请求返回 409）；
   历史轮次保存在进程内存（最近 50 轮），重启进程后清空，未做持久化。
   冲销记录写入 PostgreSQL，进程重启后仍可通过接口查询（但不在历史列表中回显）。
4. 库存表仅两条样例记录、调拨数量写死，聚焦锁等待演示，未做通用库存建模；
   冲销也只能按样例调拨单整笔反向。
5. 冲销的“受理/重置先后”依赖单进程内的条件锁；若把 gunicorn 扩成多 worker，
   需要把受理状态迁移到数据库（如 advisory lock）才能保持同样的语义。
