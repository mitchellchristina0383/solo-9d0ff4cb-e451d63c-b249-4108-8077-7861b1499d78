"""
lock-lab —— 库位调拨行锁互斥实验（Flask + PostgreSQL）

两笔调拨单各开一个独立连接/事务，对同两条 inventory 记录执行
SELECT ... FOR UPDATE。可选择：
  * same     相同加锁次序：两笔都按 location_id 升序加锁 -> 普通等待，不会死锁
  * opposite 相反加锁次序：T1 先 A-01 后 A-02，T2 先 A-02 后 A-01 -> 数据库可报死锁

死锁只认 PostgreSQL 返回的 SQLSTATE 40P01，不自行推断。
"""

import logging
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

import psycopg2
from flask import Flask, jsonify, render_template, request

from wait_snapshot import fetch_inventory, fetch_lock_waits

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("lock-lab")

app = Flask(__name__)

DB_CONF = {
    "host": os.environ.get("PGHOST", "db"),
    "port": int(os.environ.get("PGPORT", "5432")),
    "dbname": os.environ.get("PGDATABASE", "locklab"),
    "user": os.environ.get("PGUSER", "locklab"),
    "password": os.environ.get("PGPASSWORD", "locklab"),
}

TRANSFERS = {
    "T1": {"id": "TR-1001", "from": "A-01", "to": "A-02", "qty": 10},
    "T2": {"id": "TR-1002", "from": "A-02", "to": "A-01", "qty": 10},
}
ORDERED_LOCATIONS = ["A-01", "A-02"]

# 快照最多保留多少条，避免长时间等待时内存无限增长
MAX_SNAPSHOTS = 600
# 等待关系采样间隔（秒）
SAMPLE_INTERVAL = 0.1
# 历史轮次最多保留多少轮
MAX_HISTORY = 50

# 全局只允许一轮在跑（等待关系必须来自同一批连接才好复盘）
_manager_lock = threading.Lock()
# 冲销受理与新一轮受理共用同一把条件锁：
#   * 先拿到锁的一方的“受理”生效；另一方随后看到的都是已更新的受理状态
#   * 新一轮的 reset_inventory() 必须在所有已受理冲销事务结束后才执行
_manager_cond = threading.Condition(_manager_lock)
_active_run = None          # type: ignore[var-annotated]
_runs: dict[str, "Run"] = {}
# 最近一次被受理的轮次；旧轮不再接受“新的”冲销
_latest_accepted_run_id: str | None = None
# 已被受理、事务尚未结束的冲销数量（新一轮重置库存前必须等它归零）
_reversals_inflight = 0

# 与 db/init.sql 保持一致；应用启动时再执行一次，兼容已存在的 PG 数据卷
# （官方 postgres 镜像只在空数据目录时执行 /docker-entrypoint-initdb.d）。
REVERSALS_DDL = """
CREATE TABLE IF NOT EXISTS reversals (
    reversal_id    BIGSERIAL PRIMARY KEY,
    run_id         TEXT        NOT NULL,
    transfer_key   TEXT        NOT NULL,
    transfer_id    TEXT        NOT NULL,
    from_location  TEXT        NOT NULL,
    to_location    TEXT        NOT NULL,
    qty            INTEGER     NOT NULL CHECK (qty > 0),
    from_qty_after INTEGER,
    to_qty_after   INTEGER,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, transfer_key)
);
CREATE INDEX IF NOT EXISTS idx_reversals_run_id ON reversals (run_id);
"""

_schema_lock = threading.Lock()
_schema_ready = False


class ReversalError(Exception):
    """冲销请求在业务上被拒绝（页面按 409 展示）。"""


def ensure_reversals_schema():
    """幂等建冲销表；旧数据卷也能在不删库的情况下升级。"""
    global _schema_ready
    if _schema_ready:
        return
    with _schema_lock:
        if _schema_ready:
            return
        conn = connect("locklab-schema")
        try:
            with conn.cursor() as cur:
                cur.execute(REVERSALS_DDL)
            conn.commit()
            _schema_ready = True
        finally:
            conn.close()


def wait_for_inflight_reversals():
    """新一轮重置库存前，等所有已受理的冲销事务真正结束。"""
    with _manager_cond:
        while _reversals_inflight > 0:
            log.info("inventory reset waits for %d in-flight reversal(s)", _reversals_inflight)
            _manager_cond.wait()


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def connect(app_name: str):
    conn = psycopg2.connect(application_name=app_name, **DB_CONF)
    conn.autocommit = False
    return conn


# --------------------------------------------------------------------------- #
# 一轮实验的状态容器
# --------------------------------------------------------------------------- #
@dataclass
class Run:
    run_id: str
    lock_order: str
    lock_timeout_ms: int
    deadlock_timeout_ms: int
    stagger_ms: int
    hard_timeout_s: float
    status: str = "running"                 # running / finished / cancelled
    error: str | None = None
    created_at: str = field(default_factory=now_iso)
    finished_at: str | None = None
    start_ts: float = 0.0
    snapshots: list[dict] = field(default_factory=list)
    workers: dict = field(default_factory=dict)
    cancel_event: threading.Event = field(default_factory=threading.Event)
    conns: dict = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)
    inventory_after: list[dict] | None = None
    wait_cycle: list[str] | None = None
    deadlock_victim: str | None = None
    db_declared_deadlock: bool = False
    reversals: dict[str, dict] = field(default_factory=dict)

    def public(self):
        with self.lock:
            return {
                "run_id": self.run_id,
                "lock_order": self.lock_order,
                "lock_timeout_ms": self.lock_timeout_ms,
                "deadlock_timeout_ms": self.deadlock_timeout_ms,
                "stagger_ms": self.stagger_ms,
                "hard_timeout_s": self.hard_timeout_s,
                "status": self.status,
                "error": self.error,
                "created_at": self.created_at,
                "finished_at": self.finished_at,
                "snapshots": self.snapshots,
                "workers": self.workers,
                "inventory_after": self.inventory_after,
                "wait_cycle": self.wait_cycle,
                "deadlock_victim": self.deadlock_victim,
                "db_declared_deadlock": self.db_declared_deadlock,
                "reversals": self.reversals,
                # 页面据此提示旧轮不能再接受新冲销（服务端仍会独立校验）
                "is_latest_accepted": _latest_accepted_run_id == self.run_id,
                "transfers": TRANSFERS,
            }

    def add_snapshot(self, snap: dict):
        with self.lock:
            if len(self.snapshots) < MAX_SNAPSHOTS:
                self.snapshots.append(snap)

    def update_worker(self, key: str, patch: dict):
        with self.lock:
            self.workers.setdefault(key, {})
            self.workers[key].update(patch)


# --------------------------------------------------------------------------- #
# 单笔调拨工作线程
# --------------------------------------------------------------------------- #
def lock_sequence(key: str, lock_order: str) -> list[str]:
    """返回该调拨单的实际加锁顺序。"""
    if lock_order == "same":
        # 两笔都按统一的库位顺序加锁 -> 不会形成等待环
        return list(ORDERED_LOCATIONS)
    # opposite：按调拨方向“先锁源库位、再锁目标库位”，两笔方向相反
    t = TRANSFERS[key]
    return [t["from"], t["to"]]


def worker(run: Run, key: str, barrier: threading.Barrier):
    """
    每个调拨单一条独立数据库连接、一个事务：
      BEGIN -> 逐行 SELECT ... FOR UPDATE -> UPDATE 两条库存 -> COMMIT
    """
    t = TRANSFERS[key]
    app_name = f"locklab-{key}"
    state = {
        "key": key,
        "transfer_id": t["id"],
        "qty": t["qty"],
        "app_name": app_name,
        "pid": None,
        "phase": "connecting",
        "locks_acquired": [],
        "result": None,          # committed / rolled_back
        "sqlstate": None,
        "message": None,
        "timeline": [],
    }
    run.update_worker(key, state)

    def note(phase: str):
        state["phase"] = phase
        state["timeline"].append({"t": round(time.time() - run.start_ts, 3), "phase": phase})
        run.update_worker(key, state)

    conn = None
    try:
        conn = connect(app_name)
        run.conns[key] = conn
        with conn.cursor() as cur:
            cur.execute("SELECT pg_backend_pid()")
            state["pid"] = cur.fetchone()[0]
            run.update_worker(key, state)

            cur.execute("SET lock_timeout = %s", (run.lock_timeout_ms,))
            cur.execute("SET deadlock_timeout = %s", (run.deadlock_timeout_ms,))
            note("begin")
            cur.execute("BEGIN")

            seq = lock_sequence(key, run.lock_order)
            state["lock_sequence"] = seq
            run.update_worker(key, state)

            # 三方屏障：两个工作线程与监控线程在此同时起跑，
            # 保证从“首锁请求”那一刻起监控就在采样等待关系
            note("ready_waiting_barrier")
            barrier.wait()

            for idx, loc in enumerate(seq):
                if run.cancel_event.is_set():
                    raise RuntimeError("run cancelled before locking %s" % loc)
                note("requesting_lock:" + loc)
                cur.execute(
                    "SELECT qty_on_hand FROM inventory "
                    "WHERE location_id = %s FOR UPDATE",
                    (loc,),
                )
                cur.fetchone()
                note("lock_acquired:" + loc)
                state["locks_acquired"].append(loc)
                run.update_worker(key, state)

                # 拿到第一把锁后停顿，保证两笔都先持有一把再去抢对方那把，
                # 相反次序下等待环稳定出现；相同次序下表现为排队等待
                if idx == 0 and not run.cancel_event.is_set():
                    if run.cancel_event.wait(run.stagger_ms / 1000.0):
                        raise RuntimeError("run cancelled during stagger")

            note("updating")
            cur.execute(
                "UPDATE inventory SET qty_on_hand = qty_on_hand - %s, "
                "updated_at = now() WHERE location_id = %s",
                (t["qty"], t["from"]),
            )
            cur.execute(
                "UPDATE inventory SET qty_on_hand = qty_on_hand + %s, "
                "updated_at = now() WHERE location_id = %s",
                (t["qty"], t["to"]),
            )
            note("committing")
            conn.commit()
            state["result"] = "committed"
            state["message"] = "事务已提交"
            note("committed")
    except threading.BrokenBarrierError:
        # 屏障在异常路径下被重置时，工作线程直接退出即可
        log.warning("worker %s barrier broken, exiting", key)
        state["result"] = "rolled_back"
        state["message"] = "同步屏障异常，工作线程退出"
    except Exception as exc:  # noqa: BLE001 - 需把所有结局如实回传页面
        sqlstate = getattr(exc, "pgcode", None)
        state["sqlstate"] = sqlstate
        if conn is not None:
            try:
                conn.rollback()
            except Exception:  # pragma: no cover
                pass
        if sqlstate == "40P01":
            # 唯一的死锁判定来源：数据库自己报 deadlock detected
            state["result"] = "rolled_back"
            state["message"] = "PostgreSQL 检测到死锁，本事务被数据库回滚（40P01）"
            note("deadlock_rollback")
            run.db_declared_deadlock = True
            run.deadlock_victim = key
        elif sqlstate == "55P03":
            state["result"] = "rolled_back"
            state["message"] = "等待行锁超过 lock_timeout，本事务回滚（55P03）"
            note("lock_timeout_rollback")
        else:
            state["result"] = "rolled_back"
            state["message"] = f"{type(exc).__name__}: {exc}"
            note("error_rollback")
        log.warning("worker %s ended: %s", key, state["message"])
    finally:
        state["phase"] = "done"
        run.update_worker(key, state)
        # 无论成功失败，本轮连接必须释放回操作系统
        if conn is not None:
            try:
                conn.close()
            except Exception:  # pragma: no cover
                pass
            run.conns.pop(key, None)


# --------------------------------------------------------------------------- #
# 监控线程：周期读取数据库里真实的等待关系
# --------------------------------------------------------------------------- #
def monitor(run: Run, barrier: threading.Barrier):
    try:
        barrier.wait()
    except threading.BrokenBarrierError:
        log.warning("monitor barrier broken, exiting")
        return
    admin = connect("locklab-monitor")
    sample_no = 0
    try:
        while True:
            time.sleep(SAMPLE_INTERVAL)
            sample_no += 1
            try:
                edges = fetch_lock_waits(admin)
                _annotate_waited_locations(run, edges)
            except Exception as exc:  # noqa: BLE001
                log.warning("snapshot failed: %s", exc)
                edges = []
            snap = {
                "t": round(time.time() - run.start_ts, 3),
                "sample": sample_no,
                "edges": edges,
            }
            run.add_snapshot(snap)

            # 硬性兜底超时：取消并强制 terminate 本轮连接，避免遗留
            if time.time() - run.start_ts > run.hard_timeout_s:
                run.error = (
                    f"硬性超时 {run.hard_timeout_s:.0f}s 到达，"
                    "已取消本轮并强制释放数据库连接"
                )
                run.cancel_event.set()
                _terminate_run_connections(run, admin)
                break

            if all(w.get("phase") == "done" for w in run.workers.values()) \
                    and len(run.workers) == 2:
                break
    finally:
        try:
            admin.close()
        except Exception:  # pragma: no cover
            pass


# 各工作线程“当前正在请求的库位”，用于在数据库视图未带 tuple 信息时补注
# （PG15 行锁等待者只有 transactionid/ShareLock，拿不到具体行）
def _annotate_waited_locations(run: Run, edges: list[dict]):
    for e in edges:
        if e.get("waited_location"):
            continue
        app = e.get("blocked_app", "")
        key = app.replace("locklab-", "")
        w = run.workers.get(key)
        if w and w.get("phase", "").startswith("requesting_lock:"):
            e["waited_location"] = w["phase"].split(":", 1)[1]
            e["location_source"] = "app-annotation"


def _terminate_run_connections(run: Run, admin_conn):
    """超时兜底：按 pid 终止本轮两个后端，确保连接被释放。"""
    with admin_conn.cursor() as cur:
        for key in list(run.conns.keys()):
            conn = run.conns.get(key)
            pid = run.workers.get(key, {}).get("pid")
            if pid:
                cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
                log.warning("hard timeout: terminated backend pid=%s (%s)", pid, key)
            if conn is not None:
                try:
                    conn.close()
                except Exception:  # pragma: no cover
                    pass
            run.conns.pop(key, None)
    admin_conn.commit()


# --------------------------------------------------------------------------- #
# 一轮实验的编排
# --------------------------------------------------------------------------- #
def reset_inventory():
    # 已受理的冲销还在事务里时不能重置，否则会把冲销的写入覆盖掉/交错
    wait_for_inflight_reversals()
    conn = connect("locklab-reset")
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE inventory SET qty_on_hand = 100, updated_at = now() "
                "WHERE location_id IN ('A-01','A-02')"
            )
        conn.commit()
    finally:
        conn.close()


def derive_wait_cycle(run: Run):
    """从数据库观测到的等待边里找环（仅用于页面高亮，死锁判定仍只看 40P01）。"""
    graph: dict[str, set[str]] = {}
    for snap in run.snapshots:
        for e in snap["edges"]:
            graph.setdefault(e["blocked_app"], set()).add(e["blocking_app"])
    for start in graph:
        stack = [(start, [start])]
        while stack:
            node, path = stack.pop()
            for nxt in graph.get(node, ()):  # type: ignore[union-attr]
                if nxt == start and len(path) >= 2:
                    return path + [start]
                if nxt not in path:
                    stack.append((nxt, path + [nxt]))
    return None


def run_supervisor(run: Run):
    global _active_run
    try:
        ensure_reversals_schema()
        reset_inventory()
        run.start_ts = time.time()
        barrier = threading.Barrier(3)
        threads = [
            threading.Thread(target=worker, args=(run, "T1", barrier), name="T1"),
            threading.Thread(target=worker, args=(run, "T2", barrier), name="T2"),
            threading.Thread(target=monitor, args=(run, barrier), name="monitor"),
        ]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        # 监控线程可能因硬超时提前退出；兜底再收一次
        run.cancel_event.set()

        admin = connect("locklab-finalize")
        try:
            run.inventory_after = fetch_inventory(admin)
        finally:
            admin.close()

        run.wait_cycle = derive_wait_cycle(run)
        with run.lock:
            if run.status == "running":
                run.status = "cancelled" if run.error else "finished"
            run.finished_at = now_iso()
    except Exception as exc:  # noqa: BLE001
        log.exception("supervisor failed")
        run.error = f"编排异常: {exc}"
        run.status = "cancelled"
        run.finished_at = now_iso()
    finally:
        with _manager_lock:
            _active_run = None
        with run.lock:
            if run.finished_at is None:
                run.finished_at = now_iso()
        _runs[run.run_id] = run
        # 控制历史长度
        if len(_runs) > MAX_HISTORY:
            for old_id in sorted(
                _runs, key=lambda rid: _runs[rid].created_at
            )[: len(_runs) - MAX_HISTORY]:
                _runs.pop(old_id, None)
        log.info("run %s finalized: %s", run.run_id, run.status)


# --------------------------------------------------------------------------- #
# 冲销
# --------------------------------------------------------------------------- #
def _reversal_row_to_dict(row) -> dict:
    return {
        "reversal_id": row[0],
        "run_id": row[1],
        "transfer_key": row[2],
        "transfer_id": row[3],
        "from_location": row[4],
        "to_location": row[5],
        "qty": row[6],
        "from_qty_after": row[7],
        "to_qty_after": row[8],
        "created_at": row[9].isoformat() if row[9] else None,
    }


REVERSAL_COLUMNS = (
    "reversal_id, run_id, transfer_key, transfer_id, from_location, to_location, "
    "qty, from_qty_after, to_qty_after, created_at"
)


def fetch_existing_reversal(run_id: str, key: str) -> dict | None:
    """只读查询已落账的冲销；用于重提时直接返回原结果（不再入账）。"""
    conn = connect("locklab-reversal-lookup")
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {REVERSAL_COLUMNS} FROM reversals "
                "WHERE run_id = %s AND transfer_key = %s",
                (run_id, key),
            )
            row = cur.fetchone()
        return _reversal_row_to_dict(row) if row else None
    finally:
        conn.close()


def admit_reversal(run: Run, key: str):
    """在受理锁内做准入，并占住一个 in-flight 名额直到事务结束。

    * 只有“最近受理的一轮”可以接受新冲销；
    * 该轮必须已结束（非 running）；
    * 只允许冲销本轮已确认提交的调拨单；
    * 名额与新一轮的库存重置共用 _manager_cond，保证重置一定排在其后。
    """
    global _reversals_inflight
    with _manager_cond:
        if _latest_accepted_run_id != run.run_id:
            raise ReversalError("只允许冲销最近受理的一轮；该轮之后已有新一轮被受理")
        with run.lock:
            status = run.status
            worker = dict(run.workers.get(key) or {})
        if status == "running" or run.finished_at is None:
            raise ReversalError("该轮尚未结束，暂不能冲销")
        if not worker:
            raise ReversalError(f"调拨单 {key} 不属于该轮")
        if worker.get("result") != "committed":
            raise ReversalError(
                f"调拨单 {key} 在本轮未确认提交（{worker.get('result') or '未完成'}），不能冲销"
            )
        _reversals_inflight += 1


def release_reversal_admission():
    global _reversals_inflight
    with _manager_cond:
        _reversals_inflight -= 1
        _manager_cond.notify_all()


def perform_reversal(run: Run, key: str) -> tuple[dict, bool]:
    """按原调拨数量反向调回；库存与冲销记录同一事务提交。

    返回 (冲销记录, 是否为重复提交回放)。
    """
    t = TRANSFERS[key]
    conn = connect(f"locklab-reversal-{key}")
    try:
        with conn.cursor() as cur:
            # 先抢冲销记录的插入权：唯一键保证同一轮同一单只有一个事务能拿到行。
            # 并发重复请求会阻塞在这里，等第一个事务结束后返回 0 行。
            cur.execute(
                "INSERT INTO reversals "
                "(run_id, transfer_key, transfer_id, from_location, to_location, qty) "
                "VALUES (%s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (run_id, transfer_key) DO NOTHING "
                "RETURNING reversal_id",
                (run.run_id, key, t["id"], t["from"], t["to"], t["qty"]),
            )
            inserted = cur.fetchone()

            if inserted is None:
                # 已有一笔冲销落账：不重复扣加库存，原样返回当时的冲销结果
                cur.execute(
                    f"SELECT {REVERSAL_COLUMNS} FROM reversals "
                    "WHERE run_id = %s AND transfer_key = %s",
                    (run.run_id, key),
                )
                existing = cur.fetchone()
                conn.rollback()
                if existing is None:  # 理论上不可达：冲突行一定可见
                    raise ReversalError("冲销记录冲突但读取不到原结果，请重试")
                return _reversal_row_to_dict(existing), True

            # 同一事务内反向调回：源库位加回、目标库位扣回。
            # 按库位排序加行锁，避免不同调拨单并发冲销时形成等待环。
            balances = {}
            for loc in sorted((t["from"], t["to"])):
                delta = t["qty"] if loc == t["from"] else -t["qty"]
                cur.execute(
                    "UPDATE inventory SET qty_on_hand = qty_on_hand + %s, "
                    "updated_at = now() WHERE location_id = %s "
                    "RETURNING qty_on_hand",
                    (delta, loc),
                )
                row = cur.fetchone()
                if row is None:
                    raise ReversalError(f"库位 {loc} 不存在，冲销回滚")
                balances[loc] = row[0]

            from_after = balances[t["from"]]
            to_after = balances[t["to"]]
            cur.execute(
                "UPDATE reversals SET from_qty_after = %s, to_qty_after = %s "
                "WHERE reversal_id = %s",
                (from_after, to_after, inserted[0]),
            )

            # 库存与冲销记录在同一个事务里提交；任一写入失败都会一起回滚
            conn.commit()

            cur.execute(
                f"SELECT {REVERSAL_COLUMNS} FROM reversals WHERE reversal_id = %s",
                (inserted[0],),
            )
            record = _reversal_row_to_dict(cur.fetchone())
        return record, False
    except Exception:
        try:
            conn.rollback()
        except Exception:  # pragma: no cover
            pass
        raise
    finally:
        # 连接关闭 = 事务一定已结束，之后才释放 in-flight 名额，
        # 这样新一轮的库存重置不会与冲销交错。
        conn.close()


# --------------------------------------------------------------------------- #
# HTTP 接口
# --------------------------------------------------------------------------- #
@app.route("/")
def index():
    return render_template("index.html", transfers=TRANSFERS)


@app.route("/api/runs", methods=["POST"])
def start_run():
    global _active_run, _latest_accepted_run_id
    payload = request.get_json(silent=True) or {}
    lock_order = payload.get("lock_order", "opposite")
    if lock_order not in ("same", "opposite"):
        return jsonify({"error": "lock_order 只能是 same 或 opposite"}), 400

    try:
        lock_timeout_ms = max(100, min(600000, int(payload.get("lock_timeout_ms", 15000))))
        deadlock_timeout_ms = max(50, min(60000, int(payload.get("deadlock_timeout_ms", 1000))))
        stagger_ms = max(0, min(10000, int(payload.get("stagger_ms", 1200))))
        hard_timeout_s = max(5.0, min(300.0, float(payload.get("hard_timeout_s", 30.0))))
    except (TypeError, ValueError):
        return jsonify({"error": "参数格式不正确"}), 400

    with _manager_lock:
        if _active_run is not None and _active_run.status == "running":
            return jsonify({"error": "已有一轮实验在运行，请先取消或等待结束",
                            "run_id": _active_run.run_id}), 409
        run = Run(
            run_id=uuid.uuid4().hex[:12],
            lock_order=lock_order,
            lock_timeout_ms=lock_timeout_ms,
            deadlock_timeout_ms=deadlock_timeout_ms,
            stagger_ms=stagger_ms,
            hard_timeout_s=hard_timeout_s,
            workers={},
        )
        _active_run = run
        _runs[run.run_id] = run
        # 新一轮一受理，旧轮立刻失去“新冲销”的资格
        _latest_accepted_run_id = run.run_id

    threading.Thread(target=run_supervisor, args=(run,), name="supervisor", daemon=True).start()
    return jsonify({"run_id": run.run_id}), 201


@app.route("/api/runs/<run_id>")
def get_run(run_id):
    run = _runs.get(run_id)
    if run is None:
        return jsonify({"error": "轮次不存在"}), 404
    return jsonify(run.public())


@app.route("/api/runs/<run_id>/reversals", methods=["POST"])
def reverse_transfer(run_id):
    """冲销本轮某一笔已确认提交的调拨单（按原数量反向调回）。"""
    run = _runs.get(run_id)
    if run is None:
        return jsonify({"error": "轮次不存在"}), 404

    payload = request.get_json(silent=True) or {}
    key = payload.get("transfer_key")
    if key not in TRANSFERS:
        return jsonify({"error": "transfer_key 只能是 T1 或 T2"}), 400

    try:
        ensure_reversals_schema()
        existing = fetch_existing_reversal(run.run_id, key)
    except Exception as exc:  # noqa: BLE001
        log.exception("reversal pre-check failed")
        return jsonify({"error": f"冲销前检查失败: {exc}"}), 500

    if existing is not None:
        # 幂等重提：无论之后是否已有新一轮，都不再入账，返回原冲销结果
        with run.lock:
            run.reversals[key] = existing
        body = {"ok": True, "duplicate": True, "reversal": existing,
                "transfers": TRANSFERS, "inventory": run.inventory_after}
        return jsonify(body), 200

    if _latest_accepted_run_id != run.run_id:
        return jsonify({"error": "只允许冲销最近受理的一轮；该轮之后已有新一轮被受理"}), 409

    try:
        admit_reversal(run, key)
    except ReversalError as exc:
        return jsonify({"error": str(exc)}), 409
    except Exception as exc:  # noqa: BLE001
        log.exception("reversal admission failed")
        return jsonify({"error": f"冲销受理失败: {exc}"}), 500

    try:
        record, duplicate = perform_reversal(run, key)
        with run.lock:
            run.reversals[key] = record
        # 冲销提交后刷新页面展示的库存实况（原调拨结局 workers 保持不动）。
        # 这只是展示用快照，失败也不应把已提交的冲销报成失败。
        inventory = run.inventory_after
        try:
            inv_conn = connect("locklab-reversal-view")
            try:
                inventory = fetch_inventory(inv_conn)
            finally:
                inv_conn.close()
            run.inventory_after = inventory
        except Exception:  # noqa: BLE001
            log.warning("refresh inventory after reversal failed", exc_info=True)
        log.info("reversal %s run=%s key=%s duplicate=%s",
                 record["reversal_id"], run_id, key, duplicate)
    except ReversalError as exc:
        return jsonify({"error": str(exc)}), 409
    except Exception as exc:  # noqa: BLE001
        log.exception("reversal failed")
        return jsonify({"error": f"冲销失败，事务已回滚: {exc}"}), 500
    finally:
        release_reversal_admission()

    body = {"ok": True, "duplicate": duplicate, "reversal": record,
            "transfers": TRANSFERS, "inventory": inventory}
    # 首次入账 201；并发重复提交（被数据库唯一键挡住）返回 200 + 原冲销结果
    return jsonify(body), 200 if duplicate else 201


@app.route("/api/runs/<run_id>/cancel", methods=["POST"])
def cancel_run(run_id):
    run = _runs.get(run_id)
    if run is None:
        return jsonify({"error": "轮次不存在"}), 404
    run.cancel_event.set()
    run.error = (run.error or "") + "；用户手动取消，正在释放本轮连接"
    # 让被 lock_timeout 之外阻塞的语句也能尽快退出
    for key, conn in list(run.conns.items()):
        pid = run.workers.get(key, {}).get("pid")
        if pid:
            try:
                admin = connect("locklab-cancel")
                try:
                    with admin.cursor() as cur:
                        cur.execute("SELECT pg_cancel_backend(%s)", (pid,))
                    admin.commit()
                finally:
                    admin.close()
            except Exception as exc:  # noqa: BLE001
                log.warning("cancel failed for %s: %s", pid, exc)
    return jsonify({"ok": True})


@app.route("/api/runs")
def list_runs():
    items = [
        {
            "run_id": r.run_id,
            "lock_order": r.lock_order,
            "status": r.status,
            "created_at": r.created_at,
            "db_declared_deadlock": r.db_declared_deadlock,
            "deadlock_victim": r.deadlock_victim,
        }
        for r in sorted(_runs.values(), key=lambda x: x.created_at, reverse=True)
    ]
    return jsonify(items)


@app.route("/api/health")
def health():
    try:
        conn = connect("locklab-health")
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        finally:
            conn.close()
        return jsonify({"ok": True})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 503


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded=True)
