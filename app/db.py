import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path

from app.config import Config
from app import merge as merge_logic

SEED_PIPELINES = [
    ("订单明细日抽", "orders_daily", "待抽取", "mock://orders/day"),
    ("用户画像宽表", "user_profile", "待抽取", "mock://users/profile"),
    ("库存快照落地", "inventory_snap", "待抽取", "mock://stock/snap"),
    ("支付流水清洗", "pay_clean", "待抽取", "mock://pay/flow"),
]

ROLES = (merge_logic.ROLE_PRIMARY, merge_logic.ROLE_SECONDARY)

# 业务拒绝码：控制器据此映射 HTTP 状态码。
ERR_NOT_FOUND = 404
ERR_BAD_REQUEST = 400
ERR_CONFLICT = 409


def connect(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or Config.database()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 15000")
    return conn


@contextmanager
def get_db(db_path: str | None = None):
    conn = connect(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _today() -> str:
    return date.today().isoformat()


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _ensure_columns(conn: sqlite3.Connection) -> None:
    existing = {r["name"] for r in conn.execute("PRAGMA table_info(pipelines)").fetchall()}
    additions = {
        "merge_key": "TEXT NOT NULL DEFAULT ''",
        "conflict_policy": "TEXT NOT NULL DEFAULT ''",
        "policy_locked_code": "TEXT NOT NULL DEFAULT ''",
        "policy_locked_at": "TEXT NOT NULL DEFAULT ''",
        "merge_status": "TEXT NOT NULL DEFAULT '未合并'",
        "merged_at": "TEXT NOT NULL DEFAULT ''",
        "merged_count": "INTEGER NOT NULL DEFAULT 0",
    }
    for column, ddl in additions.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE pipelines ADD COLUMN {column} {ddl}")


def init_db(db_path: str | None = None) -> None:
    with get_db(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS pipelines (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              title TEXT NOT NULL,
              code TEXT NOT NULL UNIQUE,
              status TEXT NOT NULL,
              source_uri TEXT NOT NULL,
              extracted INTEGER NOT NULL DEFAULT 0,
              loaded INTEGER NOT NULL DEFAULT 0,
              created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS meta (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );
            """
        )
        _ensure_columns(conn)
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS pipeline_sources (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              pipeline_id INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
              role TEXT NOT NULL,
              source_uri TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT '未抽取',
              extracted INTEGER NOT NULL DEFAULT 0,
              extracted_at TEXT NOT NULL DEFAULT '',
              UNIQUE(pipeline_id, role)
            );
            CREATE TABLE IF NOT EXISTS merge_runs (
              pipeline_id INTEGER PRIMARY KEY REFERENCES pipelines(id) ON DELETE CASCADE,
              token TEXT NOT NULL,
              user TEXT NOT NULL,
              status TEXT NOT NULL,
              created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS merge_claims (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              pipeline_id INTEGER NOT NULL,
              token TEXT NOT NULL DEFAULT '',
              user TEXT NOT NULL,
              outcome TEXT NOT NULL,
              reason TEXT NOT NULL DEFAULT '',
              created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS merge_conflicts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              pipeline_id INTEGER NOT NULL,
              run_token TEXT NOT NULL DEFAULT '',
              key_value TEXT NOT NULL,
              policy TEXT NOT NULL,
              resolution TEXT NOT NULL,
              primary_json TEXT NOT NULL,
              secondary_json TEXT NOT NULL,
              diffs_json TEXT NOT NULL,
              note TEXT NOT NULL DEFAULT '',
              created_at TEXT NOT NULL
            );
            """
        )
        n = conn.execute("SELECT COUNT(*) AS c FROM pipelines").fetchone()["c"]
        if n == 0:
            conn.executemany(
                "INSERT INTO pipelines (title, code, status, source_uri) VALUES (?,?,?,?)",
                SEED_PIPELINES,
            )
            conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('lab_name', ?)",
                (Config.LAB_NAME,),
            )
        rows = conn.execute("SELECT id, source_uri FROM pipelines").fetchall()
        for row in rows:
            for role in ROLES:
                uri = row["source_uri"]
                if role == merge_logic.ROLE_SECONDARY:
                    uri = f"{uri}?role=secondary"
                conn.execute(
                    "INSERT OR IGNORE INTO pipeline_sources "
                    "(pipeline_id, role, source_uri, status, extracted) VALUES (?,?,?,?,0)",
                    (row["id"], role, uri, "未抽取"),
                )


_PIPELINE_COLUMNS = (
    "id, title, code, status, source_uri, extracted, loaded, created_at, "
    "merge_key, conflict_policy, policy_locked_code, policy_locked_at, "
    "merge_status, merged_at, merged_count"
)


def list_pipelines(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        f"SELECT {_PIPELINE_COLUMNS} FROM pipelines ORDER BY id"
    ).fetchall()
    return [dict(r) for r in rows]


def get_pipeline(conn: sqlite3.Connection, pipeline_id: int) -> dict | None:
    row = conn.execute(
        f"SELECT {_PIPELINE_COLUMNS} FROM pipelines WHERE id = ?",
        (pipeline_id,),
    ).fetchone()
    return dict(row) if row else None


def get_pipeline_by_code(conn: sqlite3.Connection, code: str) -> dict | None:
    row = conn.execute(
        f"SELECT {_PIPELINE_COLUMNS} FROM pipelines WHERE code = ?",
        (code,),
    ).fetchone()
    return dict(row) if row else None


def _strategy_of(pipeline: dict) -> dict:
    code = (
        merge_logic.normalize_policy(pipeline.get("conflict_policy"))
        or merge_logic.DEFAULT_POLICY
    )
    return {"code": code, "name": merge_logic.policy_label(code)}


# ---------------------------------------------------------------------------
# 源对照册
# ---------------------------------------------------------------------------


def get_sources(conn: sqlite3.Connection, pipeline_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT role, source_uri, status, extracted, extracted_at "
        "FROM pipeline_sources WHERE pipeline_id = ?",
        (pipeline_id,),
    ).fetchall()
    by_role = {r["role"]: dict(r) for r in rows}
    return [by_role[role] for role in ROLES if role in by_role]


def _source_rows(pipeline: dict, source: dict) -> list[dict]:
    if source["status"] != "已抽取":
        return []
    return merge_logic.build_source_rows(pipeline, source["role"])


def _extract_source(conn: sqlite3.Connection, pipeline: dict, role: str) -> dict:
    source = conn.execute(
        "SELECT role, source_uri, status, extracted, extracted_at "
        "FROM pipeline_sources WHERE pipeline_id = ? AND role = ?",
        (pipeline["id"], role),
    ).fetchone()
    if not source:
        raise KeyError(role)
    rows = merge_logic.build_source_rows(pipeline, role)
    conn.execute(
        "UPDATE pipeline_sources SET status = ?, extracted = ?, extracted_at = ? "
        "WHERE pipeline_id = ? AND role = ?",
        ("已抽取", len(rows), _now(), pipeline["id"], role),
    )
    # 新一轮抽取即开启新一轮合并，清掉上一轮争抢与冲突，保证“只成先到那笔”只作用于同一轮。
    conn.execute("DELETE FROM merge_runs WHERE pipeline_id = ?", (pipeline["id"],))
    conn.execute("DELETE FROM merge_conflicts WHERE pipeline_id = ?", (pipeline["id"],))
    conn.execute(
        "UPDATE pipelines SET merge_status = '未合并', merged_at = '', merged_count = 0 "
        "WHERE id = ?",
        (pipeline["id"],),
    )
    sources = get_sources(conn, pipeline["id"])
    if all(s["status"] == "已抽取" for s in sources):
        total = sum(s["extracted"] for s in sources)
        conn.execute(
            "UPDATE pipelines SET status = '已抽取待加载', extracted = ?, loaded = 0 "
            "WHERE id = ?",
            (total, pipeline["id"]),
        )
    return {
        "role": role,
        "source_uri": source["source_uri"],
        "status": "已抽取",
        "extracted": len(rows),
        "extracted_at": _now(),
    }


def run_source_extract(
    conn: sqlite3.Connection, pipeline_id: int, role: str
) -> dict:
    pipeline = get_pipeline(conn, pipeline_id)
    if not pipeline:
        return {"ok": False, "status": ERR_NOT_FOUND, "error": "管道不存在"}
    normalized = merge_logic.normalize_role(role)
    if normalized not in ROLES:
        return {"ok": False, "status": ERR_BAD_REQUEST, "error": "源角色无效"}
    source = _extract_source(conn, pipeline, normalized)
    return {
        "ok": True,
        "source": source,
        "sources": get_sources(conn, pipeline_id),
        "pipeline": get_pipeline(conn, pipeline_id),
    }


# ---------------------------------------------------------------------------
# 冲突设置 / 策略锁
# ---------------------------------------------------------------------------


def update_merge_config(
    conn: sqlite3.Connection, pipeline_id: int, body: dict
) -> dict:
    pipeline = get_pipeline(conn, pipeline_id)
    if not pipeline:
        return {"ok": False, "status": ERR_NOT_FOUND, "error": "管道不存在"}

    merge_key = body.get("merge_key", pipeline["merge_key"])
    merge_key = "" if merge_key is None else str(merge_key).strip()

    requested_policy = None
    if "conflict_policy" in body or "policy" in body:
        raw_policy = body.get("conflict_policy", body.get("policy"))
        requested_policy = merge_logic.normalize_policy(raw_policy)
        if requested_policy is None:
            return {
                "ok": False,
                "status": ERR_BAD_REQUEST,
                "error": "冲突策略无效",
                "detail": "仅支持整行失败进冲突筐或保留主源值并旁注次源差异",
            }

    locked_code = merge_logic.normalize_policy(pipeline.get("policy_locked_code"))
    locked_at = (pipeline.get("policy_locked_at") or "").strip()

    if (
        requested_policy
        and locked_code
        and locked_at == _today()
        and requested_policy != locked_code
    ):
        return {
            "ok": False,
            "status": ERR_CONFLICT,
            "error": "键冲突策略当天已锁定，不得更换",
            "detail": (
                f"锁定策略为「{merge_logic.policy_label(locked_code)}」，当天只能沿用该策略"
            ),
        }

    effective_policy = (
        requested_policy
        or merge_logic.normalize_policy(pipeline.get("conflict_policy"))
        or merge_logic.DEFAULT_POLICY
    )

    new_locked_code = locked_code
    new_locked_at = locked_at
    if requested_policy and not locked_code:
        new_locked_code = requested_policy
        new_locked_at = _today()
    elif requested_policy and locked_at != _today():
        # 隔天允许改口径，改完重新锁到当天。
        new_locked_code = requested_policy
        new_locked_at = _today()

    conn.execute(
        "UPDATE pipelines SET merge_key = ?, conflict_policy = ?, "
        "policy_locked_code = ?, policy_locked_at = ? WHERE id = ?",
        (merge_key, effective_policy, new_locked_code, new_locked_at, pipeline_id),
    )
    return {
        "ok": True,
        "merge_key": merge_key,
        "strategy": {
            "code": effective_policy,
            "name": merge_logic.policy_label(effective_policy),
        },
        "locked_code": new_locked_code,
        "locked_at": new_locked_at,
        "pipeline": get_pipeline(conn, pipeline_id),
    }


def _drift(pipeline: dict) -> str | None:
    effective = (
        merge_logic.normalize_policy(pipeline.get("conflict_policy"))
        or merge_logic.DEFAULT_POLICY
    )
    locked = merge_logic.normalize_policy(pipeline.get("policy_locked_code"))
    if locked and locked != effective:
        return (
            "合并策略走歪了：锁定策略为"
            f"「{merge_logic.policy_label(locked)}」，当前生效为"
            f"「{merge_logic.policy_label(effective)}」"
        )
    return None


def _blockers(conn: sqlite3.Connection, pipeline: dict) -> list[str]:
    blockers: list[str] = []
    if not (pipeline.get("merge_key") or "").strip():
        blockers.append("合并键字段为空，禁止开跑")
    sources = get_sources(conn, pipeline["id"])
    extracted_roles = {s["role"] for s in sources if s["status"] == "已抽取"}
    for role in ROLES:
        if role not in extracted_roles:
            blockers.append(f"{role}尚未抽取成功，两源未齐不能合并")
    drift = _drift(pipeline)
    if drift:
        blockers.append(drift)
    return blockers


# ---------------------------------------------------------------------------
# 合并预览 / 总览
# ---------------------------------------------------------------------------


_EMPTY_SOURCE = {"role": "", "source_uri": "", "status": "未抽取", "extracted": 0}


def build_preview(conn: sqlite3.Connection, pipeline: dict) -> dict:
    sources = get_sources(conn, pipeline["id"])
    by_role = {s["role"]: s for s in sources}
    primary_src = by_role.get(merge_logic.ROLE_PRIMARY, _EMPTY_SOURCE)
    secondary_src = by_role.get(merge_logic.ROLE_SECONDARY, _EMPTY_SOURCE)
    primary_rows = _source_rows(pipeline, primary_src)
    secondary_rows = _source_rows(pipeline, secondary_src)
    blockers = _blockers(conn, pipeline)
    return merge_logic.preview_payload(
        pipeline, primary_rows, secondary_rows, sources, blockers
    )


def merge_snapshot(conn: sqlite3.Connection, pipeline_id: int) -> dict:
    pipeline = get_pipeline(conn, pipeline_id)
    if not pipeline:
        return {"ok": False, "status": ERR_NOT_FOUND, "error": "管道不存在"}
    preview = build_preview(conn, pipeline)
    has_run = (
        conn.execute(
            "SELECT 1 FROM merge_runs WHERE pipeline_id = ? LIMIT 1",
            (pipeline_id,),
        ).fetchone()
        is not None
    )
    claims = [
        dict(r)
        for r in conn.execute(
            "SELECT id, pipeline_id, user, outcome, reason, created_at "
            "FROM merge_claims WHERE pipeline_id = ? ORDER BY id DESC LIMIT 20",
            (pipeline_id,),
        ).fetchall()
    ]
    conflicts = [
        _conflict_from_row(r)
        for r in conn.execute(
            "SELECT key_value, policy, resolution, primary_json, secondary_json, "
            "diffs_json, note FROM merge_conflicts WHERE pipeline_id = ? ORDER BY id",
            (pipeline_id,),
        ).fetchall()
    ]
    strategy = _strategy_of(pipeline)
    sources = preview["sources"]
    primary_source = next(
        (s for s in sources if s["role"] == merge_logic.ROLE_PRIMARY), {}
    )
    secondary_source = next(
        (s for s in sources if s["role"] == merge_logic.ROLE_SECONDARY), {}
    )
    return {
        "ok": True,
        "pipeline": pipeline,
        "merge_key": pipeline["merge_key"],
        "merge_status": pipeline["merge_status"],
        "merged_at": pipeline["merged_at"],
        "strategy": strategy,
        "policy_locked_code": pipeline["policy_locked_code"],
        "policy_locked_at": pipeline["policy_locked_at"],
        "sources": sources,
        "primary_source": primary_source,
        "secondary_source": secondary_source,
        "overview": {
            "primary_count": preview["primary_count"],
            "secondary_count": preview["secondary_count"],
            "conflict_count": preview["conflict_count"],
            "strategy_name": strategy["name"],
            "merge_status": pipeline["merge_status"],
        },
        "preview": {
            "primary_count": preview["primary_count"],
            "secondary_count": preview["secondary_count"],
            "conflict_count": preview["conflict_count"],
            "union_count": preview["union_count"],
            "merged_count": preview["merged_count"],
            "merged_sample": preview["merged_sample"],
            "conflicts": preview["conflicts"] or conflicts,
            "can_merge": preview["can_merge"] and not has_run,
            "blockers": preview["blockers"],
        },
        "claims": claims,
    }


def _conflict_from_row(row: sqlite3.Row) -> dict:
    return {
        "key_value": row["key_value"],
        "policy": row["policy"],
        "resolution": row["resolution"],
        "primary": json.loads(row["primary_json"]),
        "secondary": json.loads(row["secondary_json"]),
        "diffs": json.loads(row["diffs_json"]),
        "note": row["note"],
    }


def pipeline_overview(conn: sqlite3.Connection, pipeline: dict) -> dict:
    preview = build_preview(conn, pipeline)
    strategy = _strategy_of(pipeline)
    return {
        "id": pipeline["id"],
        "title": pipeline["title"],
        "code": pipeline["code"],
        "merge_key": pipeline["merge_key"],
        "strategy": strategy,
        "strategy_name": strategy["name"],
        "merge_status": pipeline["merge_status"],
        "primary_count": preview["primary_count"],
        "secondary_count": preview["secondary_count"],
        "conflict_count": preview["conflict_count"],
        "can_merge": preview["can_merge"],
        "blockers": preview["blockers"],
        "sources": [
            {
                "role": s["role"],
                "source_uri": s["source_uri"],
                "status": s["status"],
                "extracted": s["extracted"],
            }
            for s in preview["sources"]
        ],
    }


def list_overview(conn: sqlite3.Connection) -> list[dict]:
    return [pipeline_overview(conn, p) for p in list_pipelines(conn)]


# ---------------------------------------------------------------------------
# 执行合并（服务端裁决 + 原子争抢）
# ---------------------------------------------------------------------------


def run_merge(
    conn: sqlite3.Connection,
    pipeline_id: int,
    user: str,
    requested_policy=None,
    requested_key: str | None = None,
) -> dict:
    pipeline = get_pipeline(conn, pipeline_id)
    if not pipeline:
        return {"ok": False, "status": ERR_NOT_FOUND, "error": "管道不存在"}

    if not (pipeline.get("merge_key") or "").strip():
        return {
            "ok": False,
            "status": ERR_BAD_REQUEST,
            "error": "合并键字段为空，禁止开跑",
            "detail": "请先在冲突设置中填写合并键字段",
        }

    if requested_key is not None and str(requested_key).strip() != pipeline["merge_key"]:
        return {
            "ok": False,
            "status": ERR_BAD_REQUEST,
            "error": "合并键已漂移，禁止开跑",
            "detail": f"当前合并键为 {pipeline['merge_key']}",
        }

    drift = _drift(pipeline)
    requested = merge_logic.normalize_policy(requested_policy) if requested_policy else None
    effective = (
        merge_logic.normalize_policy(pipeline.get("conflict_policy"))
        or merge_logic.DEFAULT_POLICY
    )
    if requested and requested != effective:
        drift = (
            "合并策略走歪了：发起合并时携带的策略为"
            f"「{merge_logic.policy_label(requested)}」，服务端生效策略为"
            f"「{merge_logic.policy_label(effective)}」"
        )
    if drift:
        return {"ok": False, "status": ERR_BAD_REQUEST, "error": drift, "detail": drift}

    sources = get_sources(conn, pipeline_id)
    extracted_roles = {s["role"] for s in sources if s["status"] == "已抽取"}
    missing = [role for role in ROLES if role not in extracted_roles]
    if missing:
        return {
            "ok": False,
            "status": ERR_BAD_REQUEST,
            "error": "两源未齐，不能合并",
            "detail": "、".join(f"{role}尚未抽取成功" for role in missing),
            "missing": missing,
        }

    token = uuid.uuid4().hex
    try:
        conn.execute(
            "INSERT INTO merge_runs (pipeline_id, token, user, status, created_at) "
            "VALUES (?,?,?,?,?)",
            (pipeline_id, token, user, "合并中", _now()),
        )
    except sqlite3.IntegrityError:
        conn.execute(
            "INSERT INTO merge_claims (pipeline_id, token, user, outcome, reason, created_at) "
            "VALUES (?,?,?,?,?,?)",
            (pipeline_id, token, user, "抢占失败", "已有合并由他人先到", _now()),
        )
        return {
            "ok": False,
            "status": ERR_CONFLICT,
            "error": "合并已被抢占，只成先到那笔",
            "detail": "该管道本轮合并已由先到的请求占用",
        }
    except sqlite3.OperationalError as exc:
        # SQLite 写锁争用同样意味着另一笔合并先到。
        conn.rollback()
        return {
            "ok": False,
            "status": ERR_CONFLICT,
            "error": "合并已被抢占，只成先到那笔",
            "detail": f"合并锁争用：{exc}",
        }

    by_role = {s["role"]: s for s in sources}
    primary_src = by_role.get(merge_logic.ROLE_PRIMARY, _EMPTY_SOURCE)
    secondary_src = by_role.get(merge_logic.ROLE_SECONDARY, _EMPTY_SOURCE)
    primary_rows = _source_rows(pipeline, primary_src)
    secondary_rows = _source_rows(pipeline, secondary_src)
    result = merge_logic.compute_merge(
        primary_rows, secondary_rows, pipeline["merge_key"], effective
    )

    conn.execute("DELETE FROM merge_conflicts WHERE pipeline_id = ?", (pipeline_id,))
    for conflict in result["conflicts"]:
        conn.execute(
            "INSERT INTO merge_conflicts (pipeline_id, run_token, key_value, policy, "
            "resolution, primary_json, secondary_json, diffs_json, note, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                pipeline_id,
                token,
                conflict["key_value"],
                conflict["policy"],
                conflict["resolution"],
                json.dumps(conflict["primary"], ensure_ascii=False),
                json.dumps(conflict["secondary"], ensure_ascii=False),
                json.dumps(conflict["diffs"], ensure_ascii=False),
                conflict.get("note", ""),
                _now(),
            ),
        )
    conn.execute(
        "UPDATE pipelines SET merge_status = '已完成', merged_at = ?, merged_count = ? "
        "WHERE id = ?",
        (_now(), result["merged_count"], pipeline_id),
    )
    conn.execute(
        "UPDATE merge_runs SET status = '已完成' WHERE pipeline_id = ?",
        (pipeline_id,),
    )
    conn.execute(
        "INSERT INTO merge_claims (pipeline_id, token, user, outcome, reason, created_at) "
        "VALUES (?,?,?,?,?,?)",
        (pipeline_id, token, user, "抢占成功", "先到并完成合并", _now()),
    )

    return {
        "ok": True,
        "token": token,
        "pipeline": get_pipeline(conn, pipeline_id),
        "strategy": {"code": result["policy"], "name": result["policy_name"]},
        "policy": result["policy"],
        "policy_name": result["policy_name"],
        "primary_count": result["primary_count"],
        "secondary_count": result["secondary_count"],
        "conflict_count": result["conflict_count"],
        "union_count": result["union_count"],
        "merged_count": result["merged_count"],
        "merged_sample": result["merged_sample"],
        "conflicts": result["conflicts"],
        "claims": [
            dict(r)
            for r in conn.execute(
                "SELECT user, outcome, reason, created_at FROM merge_claims "
                "WHERE pipeline_id = ? ORDER BY id DESC LIMIT 20",
                (pipeline_id,),
            ).fetchall()
        ],
    }


# ---------------------------------------------------------------------------
# 基线抽取 / 落库（合并管道强制先合并）
# ---------------------------------------------------------------------------


def run_extract(conn: sqlite3.Connection, pipeline_id: int) -> dict | None:
    item = get_pipeline(conn, pipeline_id)
    if not item:
        return None
    if not (item.get("merge_key") or "").strip():
        if item["status"] not in ("待抽取", "抽取失败"):
            return item
        extracted = 12 + (pipeline_id % 5)
        conn.execute(
            "UPDATE pipelines SET status = ?, extracted = ?, loaded = 0 WHERE id = ?",
            ("已抽取待加载", extracted, pipeline_id),
        )
        return get_pipeline(conn, pipeline_id)

    # 双源管道：一次抽取把主源与次源都跑通，再进入待加载。
    sources = get_sources(conn, pipeline_id)
    for source in sources:
        _extract_source(conn, item, source["role"])
    total = sum(s["extracted"] for s in get_sources(conn, pipeline_id))
    conn.execute(
        "UPDATE pipelines SET status = ?, extracted = ?, loaded = 0 WHERE id = ?",
        ("已抽取待加载", total, pipeline_id),
    )
    return get_pipeline(conn, pipeline_id)


def run_load(conn: sqlite3.Connection, pipeline_id: int) -> dict | None:
    item = get_pipeline(conn, pipeline_id)
    if not item:
        return None

    if (item.get("merge_key") or "").strip() and item.get("merge_status") != "已完成":
        return {
            "_load_blocked": True,
            "status": ERR_CONFLICT,
            "error": "合并还没完成，不能落库",
            "detail": "请先完成双源合并后再落库",
            "pipeline": item,
        }

    if item["status"] != "已抽取待加载":
        return item
    loaded = item["merged_count"] if item.get("merge_status") == "已完成" else item["extracted"]
    conn.execute(
        "UPDATE pipelines SET status = ?, loaded = ? WHERE id = ?",
        ("已落库", loaded, pipeline_id),
    )
    return get_pipeline(conn, pipeline_id)
