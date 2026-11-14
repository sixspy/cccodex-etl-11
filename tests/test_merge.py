"""双源合并场景验收：

- 双源齐套：主源与次源各自抽取成功才可合并
- 冲突筐：整行失败策略把同键不同值整行判失败
- 留主源：保留主源值并旁注次源差异
- 策略漂移：发起合并时携带与服务端不一致的策略被拦
- 单源未齐：只有一源抽取成功时不能合并
- 落库前置：合并未完成不能落库
- 合并争抢：两个并发请求只成先到那笔
- 页面可读：总览与预览能读出冲突与主次源
"""

import threading

import pytest

from app import db as store
from app.merge import compute_merge

PRIMARY = "主源"
SECONDARY = "次源"


def _first_pipeline(authed):
    items = authed.get("/api/pipelines").json()["items"]
    return items[0]


def _extract_both(authed, pid):
    for role in (PRIMARY, SECONDARY):
        res = authed.post(f"/api/pipelines/{pid}/sources/{role}/extract")
        assert res.status_code == 200, res.text
        assert res.json()["source"]["status"] == "已抽取"


def _setup(authed, policy="whole_row_fail", key="order_id"):
    pid = _first_pipeline(authed)["id"]
    cfg = authed.post(
        f"/api/pipelines/{pid}/merge-config",
        json={"merge_key": key, "conflict_policy": policy},
    )
    assert cfg.status_code == 200, cfg.text
    _extract_both(authed, pid)
    return pid


def test_overview_shows_counts_and_strategy(authed):
    pid = _setup(authed)
    overview = authed.get(f"/api/pipelines/{pid}/overview")
    assert overview.status_code == 200
    body = overview.json()
    assert body["primary_count"] > 0
    assert body["secondary_count"] > 0
    assert body["conflict_count"] > 0
    assert body["strategy_name"] == "整行失败进冲突筐"
    # 四个中文指标齐全
    assert body["中文"] == {
        "主源条数": body["primary_count"],
        "次源条数": body["secondary_count"],
        "冲突数": body["conflict_count"],
        "策略名": body["strategy_name"],
    }
    # 源对照册两源都在
    sources = authed.get(f"/api/pipelines/{pid}/sources").json()["items"]
    assert {s["role"] for s in sources} == {PRIMARY, SECONDARY}
    assert all(s["status"] == "已抽取" for s in sources)


def test_whole_row_fail_policy_and_conflict_tray(authed):
    pid = _setup(authed, policy="whole_row_fail")
    preview = authed.get(f"/api/pipelines/{pid}/merge/preview").json()["preview"]
    assert preview["conflict_count"] > 0
    assert all(c["resolution"] == "整行失败" for c in preview["conflicts"])
    merged = authed.post(f"/api/pipelines/{pid}/merge", json={})
    assert merged.status_code == 200, merged.text
    body = merged.json()
    assert body["conflict_count"] == preview["conflict_count"]
    # 整行失败：冲突行不进入合并结果，合并后条数 = 并集 - 冲突数
    assert body["merged_count"] == body["union_count"] - body["conflict_count"]
    # 冲突筐落库后仍可从快照读出
    snap = authed.get(f"/api/pipelines/{pid}/merge").json()
    assert len(snap["preview"]["conflicts"]) == preview["conflict_count"]


def test_keep_primary_policy_annotates_secondary(authed):
    pid = _setup(authed, policy="keep_primary")
    merged = authed.post(f"/api/pipelines/{pid}/merge", json={})
    assert merged.status_code == 200, merged.text
    body = merged.json()
    assert body["policy"] == "keep_primary"
    assert body["conflict_count"] > 0
    # 保留主源：冲突行按主源值保留，合并后条数 = 并集
    assert body["merged_count"] == body["union_count"]
    for conflict in body["conflicts"]:
        assert conflict["resolution"] == "保留主源"
        assert "次源差异" in conflict["note"]
        assert conflict["diffs"]
    # 合并结果取样保留主源值
    for conflict in body["conflicts"]:
        diffs = conflict["diffs"]
        primary_row = conflict["primary"]
        for field, payload in diffs.items():
            assert primary_row[field] == payload["primary"]


def test_policy_drift_is_blocked(authed):
    pid = _setup(authed, policy="whole_row_fail")
    drift = authed.post(
        f"/api/pipelines/{pid}/merge", json={"conflict_policy": "keep_primary"}
    )
    assert drift.status_code == 400
    assert "策略走歪" in drift.json()["error"]
    # 服务端仍按锁定策略，配置不可当场改
    change = authed.post(
        f"/api/pipelines/{pid}/merge-config",
        json={"merge_key": "order_id", "conflict_policy": "keep_primary"},
    )
    assert change.status_code == 409
    assert "锁定" in change.json()["error"]


def test_single_source_not_ready_blocks_merge(authed):
    pid = _first_pipeline(authed)["id"]
    authed.post(
        f"/api/pipelines/{pid}/merge-config",
        json={"merge_key": "order_id", "conflict_policy": "whole_row_fail"},
    )
    res = authed.post(f"/api/pipelines/{pid}/sources/{PRIMARY}/extract")
    assert res.status_code == 200
    merged = authed.post(f"/api/pipelines/{pid}/merge", json={})
    assert merged.status_code == 400
    assert "两源未齐" in merged.json()["error"]
    assert SECONDARY in merged.json()["missing"]


def test_empty_merge_key_blocks_merge(authed):
    pid = _first_pipeline(authed)["id"]
    _extract_both(authed, pid)
    merged = authed.post(f"/api/pipelines/{pid}/merge", json={})
    assert merged.status_code == 400
    assert "合并键字段为空" in merged.json()["error"]


def test_load_requires_merge_complete(authed):
    pid = _setup(authed)
    blocked = authed.post(f"/api/pipelines/{pid}/load")
    assert blocked.status_code == 409
    assert "合并还没完成" in blocked.json()["error"]
    merged = authed.post(f"/api/pipelines/{pid}/merge", json={})
    assert merged.status_code == 200
    loaded = authed.post(f"/api/pipelines/{pid}/load")
    assert loaded.status_code == 200
    assert loaded.json()["status"] == "已落库"


def test_merge_race_only_first_wins(authed):
    pid = _setup(authed)
    results = []

    def attempt():
        res = authed.post(f"/api/pipelines/{pid}/merge", json={})
        results.append(res.status_code)

    threads = [threading.Thread(target=attempt) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(results) == [200, 409], results
    claims = authed.get(f"/api/pipelines/{pid}/merge").json()["claims"]
    assert {c["outcome"] for c in claims} >= {"抢占成功", "抢占失败"}


def test_merge_preview_reads_conflicts_and_claims(authed):
    pid = _setup(authed)
    authed.post(f"/api/pipelines/{pid}/merge", json={})
    snap = authed.get(f"/api/pipelines/{pid}/merge").json()
    assert snap["preview"]["conflicts"]
    assert snap["preview"]["conflicts"][0]["primary"]
    assert snap["preview"]["conflicts"][0]["secondary"]
    assert snap["claims"]
    assert snap["primary_source"]["role"] == PRIMARY
    assert snap["secondary_source"]["role"] == SECONDARY
    assert snap["overview"]["strategy_name"] == "整行失败进冲突筐"


def test_merge_logic_pure_module():
    primary = [
        {"order_id": "A-1", "amount": 100},
        {"order_id": "A-2", "amount": 200},
    ]
    secondary = [
        {"order_id": "A-1", "amount": 107},
        {"order_id": "B-1", "amount": 300},
    ]
    fail = compute_merge(primary, secondary, "order_id", "whole_row_fail")
    assert fail["conflict_count"] == 1
    assert fail["union_count"] == 3
    # 整行失败：冲突行 A-1 不入结果，剩 A-2 与次源独有 B-1
    assert fail["merged_count"] == 2
    keep = compute_merge(primary, secondary, "order_id", "keep_primary")
    assert keep["merged_count"] == 3
    assert keep["conflicts"][0]["resolution"] == "保留主源"
