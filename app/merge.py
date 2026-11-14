"""双源合并的纯逻辑：源行生成、冲突裁决、合并预览。

本模块不碰数据库，只接收两源行与合并键 / 冲突策略，返回可持久化或可预览的结果，
便于服务端与测试复用同一套裁决口径。
"""

from __future__ import annotations

POLICY_WHOLE_ROW_FAIL = "whole_row_fail"
POLICY_KEEP_PRIMARY = "keep_primary"
DEFAULT_POLICY = POLICY_WHOLE_ROW_FAIL

POLICY_LABELS = {
    POLICY_WHOLE_ROW_FAIL: "整行失败进冲突筐",
    POLICY_KEEP_PRIMARY: "保留主源值并旁注次源差异",
}

POLICY_RESOLUTIONS = {
    POLICY_WHOLE_ROW_FAIL: "整行失败",
    POLICY_KEEP_PRIMARY: "保留主源",
}

# 允许前端/测试用中文名或别名指定策略。
_POLICY_ALIASES = {
    POLICY_WHOLE_ROW_FAIL: POLICY_WHOLE_ROW_FAIL,
    "whole-row-fail": POLICY_WHOLE_ROW_FAIL,
    "fail_row": POLICY_WHOLE_ROW_FAIL,
    "整行失败": POLICY_WHOLE_ROW_FAIL,
    "整行失败进冲突筐": POLICY_WHOLE_ROW_FAIL,
    POLICY_KEEP_PRIMARY: POLICY_KEEP_PRIMARY,
    "keep-primary": POLICY_KEEP_PRIMARY,
    "保留主源": POLICY_KEEP_PRIMARY,
    "保留主源值": POLICY_KEEP_PRIMARY,
    "保留主源值并旁注次源差异": POLICY_KEEP_PRIMARY,
}

ROLE_PRIMARY = "主源"
ROLE_SECONDARY = "次源"

_ROLE_ALIASES = {
    ROLE_PRIMARY: ROLE_PRIMARY,
    "primary": ROLE_PRIMARY,
    "main": ROLE_PRIMARY,
    "master": ROLE_PRIMARY,
    ROLE_SECONDARY: ROLE_SECONDARY,
    "secondary": ROLE_SECONDARY,
    "mirror": ROLE_SECONDARY,
    "alt": ROLE_SECONDARY,
}


def normalize_policy(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return _POLICY_ALIASES.get(text) or _POLICY_ALIASES.get(text.lower())


def policy_label(code: str | None) -> str:
    return POLICY_LABELS.get(code or "", "未设置")


def policy_resolution(code: str | None) -> str:
    return POLICY_RESOLUTIONS.get(code or "", "整行失败")


def normalize_role(value: str | None) -> str | None:
    if value is None:
        return None
    return _ROLE_ALIASES.get(str(value).strip())


def _row_count(pipeline_id: int, role: str) -> int:
    if role == ROLE_PRIMARY:
        return 12 + pipeline_id % 5
    return 10 + pipeline_id % 4


def _key_prefix(pipeline: dict) -> str:
    code = str(pipeline.get("code") or "P").upper()
    return code.replace("_", "-").replace(" ", "-")


def build_source_rows(pipeline: dict, role: str) -> list[dict]:
    """生成确定性的两源 mock 行。

    两源共享前缀相同的键；主源条数 12+id%5，次源 10+id%4。
    重叠段里每隔 4 行次源金额 +7，形成稳定的同键不同值冲突。
    合并键字段为空时用占位键 ``record_id`` 仍生成行，但合并会被禁止。
    """

    key_field = (pipeline.get("merge_key") or "").strip() or "record_id"
    prefix = _key_prefix(pipeline)
    rows: list[dict] = []
    for i in range(_row_count(int(pipeline["id"]), role)):
        key_value = f"{prefix}-{i + 1:04d}"
        row = {
            key_field: key_value,
            "name": f"货品{i + 1}",
            "amount": 100 + i,
            "region": ("华东", "华南", "华北")[i % 3],
        }
        if role == ROLE_SECONDARY and i % 4 == 0:
            row["amount"] = 100 + i + 7
        rows.append(row)
    return rows


def _diff_fields(primary: dict, secondary: dict, key_field: str) -> dict:
    diffs: dict = {}
    fields = set(primary) | set(secondary)
    fields.discard(key_field)
    for field in sorted(fields):
        p_val = primary.get(field)
        s_val = secondary.get(field)
        if p_val != s_val:
            diffs[field] = {"primary": p_val, "secondary": s_val}
    return diffs


def _key_of(row: dict, key_field: str) -> str:
    value = row.get(key_field)
    return "" if value is None else str(value)


def compute_merge(
    primary_rows: list[dict],
    secondary_rows: list[dict],
    key_field: str,
    policy: str,
) -> dict:
    """按策略裁决双源冲突，返回合并结果与冲突筐条目。"""

    policy = normalize_policy(policy) or DEFAULT_POLICY
    secondary_by_key: dict[str, dict] = {}
    for row in secondary_rows:
        key = _key_of(row, key_field)
        if key:
            secondary_by_key.setdefault(key, row)

    merged: list[dict] = []
    conflicts: list[dict] = []
    matched: set[str] = set()

    for p_row in primary_rows:
        key = _key_of(p_row, key_field)
        s_row = secondary_by_key.get(key) if key else None
        if s_row is None:
            merged.append(dict(p_row))
            continue
        matched.add(key)
        diffs = _diff_fields(p_row, s_row, key_field)
        if not diffs:
            merged.append(dict(p_row))
            continue

        conflict = {
            "key_value": key,
            "primary": dict(p_row),
            "secondary": dict(s_row),
            "diffs": diffs,
            "resolution": policy_resolution(policy),
            "policy": policy,
            "policy_name": policy_label(policy),
        }
        if policy == POLICY_KEEP_PRIMARY:
            kept = dict(p_row)
            conflict["kept"] = kept
            conflict["note"] = (
                "保留主源值；次源差异："
                + "，".join(
                    f"{field}={payload['secondary']!r}（主源 {payload['primary']!r}）"
                    for field, payload in diffs.items()
                )
            )
            merged.append(kept)
        else:
            conflict["note"] = "同键不同值，整行失败并进入冲突筐"
        conflicts.append(conflict)

    for s_row in secondary_rows:
        key = _key_of(s_row, key_field)
        if not key or key in matched:
            continue
        primary_has = any(_key_of(p_row, key_field) == key for p_row in primary_rows)
        if not primary_has:
            merged.append(dict(s_row))

    union_count = len(primary_rows) + sum(
        1
        for row in secondary_rows
        if _key_of(row, key_field)
        and not any(_key_of(p, key_field) == _key_of(row, key_field) for p in primary_rows)
    )

    return {
        "policy": policy,
        "policy_name": policy_label(policy),
        "key_field": key_field,
        "primary_count": len(primary_rows),
        "secondary_count": len(secondary_rows),
        "conflict_count": len(conflicts),
        "union_count": union_count,
        "merged_count": len(merged),
        "merged_sample": merged[:5],
        "conflicts": conflicts,
    }


def preview_payload(
    pipeline: dict,
    primary_rows: list[dict],
    secondary_rows: list[dict],
    sources: list[dict],
    blockers: list[str],
) -> dict:
    key_field = (pipeline.get("merge_key") or "").strip()
    policy = normalize_policy(pipeline.get("conflict_policy")) or DEFAULT_POLICY
    if not key_field:
        result = {
            "policy": policy,
            "policy_name": policy_label(policy),
            "key_field": "",
            "primary_count": len(primary_rows),
            "secondary_count": len(secondary_rows),
            "conflict_count": 0,
            "union_count": len(primary_rows) + len(secondary_rows),
            "merged_count": 0,
            "merged_sample": [],
            "conflicts": [],
        }
    else:
        result = compute_merge(primary_rows, secondary_rows, key_field, policy)

    return {
        **result,
        "strategy": {"code": result["policy"], "name": result["policy_name"]},
        "can_merge": not blockers,
        "blockers": blockers,
        "sources": sources,
    }
