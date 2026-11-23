"""整组重放与窗口补算领域引擎。

业务语义：
  事件追踪器侧 —— 值班员一次多选若干历史事件批次（可跨日期）提交为一个“回放组”，
  引擎读取原批次记录的事件与发生时间，产出重放批次。样本暂时缺失的批次标记
  “样本缺失”，待数据恢复后办理续办，组内已完成的贡献不会重复计入。

  实时聚合器侧 —— 每个已启用聚合窗口按其窗口大小把事件落到固定时间桶，
  维护“窗口贡献台账”（窗口 × 时间桶 × 事件指纹唯一），保证同一个原始事件
  在同一个窗口只贡献一次；重放实际接受的原事件与窗口在此关联，随后修正对应
  窗口汇总。若某个时间桶仍关联未读取（样本缺失）的回放来源，该窗口补算结果
  只能保持“待验明”，不能当作完成补算。

溯源：agg_correction_sources 记录每个修正分组收到的回放组与原始批次，
可从修正结果回查同组不同日期/不同来源的贡献。
"""
from __future__ import annotations

import hashlib
import json
import statistics
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from app.database import db

# 回放组状态
GROUP_PENDING = "待处理"
GROUP_PROCESSING = "处理中"
GROUP_DONE = "已完成"

# 组内批次来源状态
SOURCE_PENDING = "待读取"
SOURCE_READ = "已读取"
SOURCE_MISSING = "样本缺失"

# 重放事件接受状态
ACCEPT_OK = "已接受"
ACCEPT_DUP = "重复跳过"

# 补算结果状态
CORRECTION_UNVERIFIED = "待验明"
CORRECTION_DONE = "已修正"

# 贡献来源类型
ORIGIN_LIVE = "LIVE"
ORIGIN_REPLAY = "REPLAY"


# ----------------------------------------------------------------------------
# 基础工具
# ----------------------------------------------------------------------------

def event_fingerprint(evt: dict[str, Any]) -> str:
    """原始事件指纹：同一事件（名称/属性/触发条件/发生时间）在重放中稳定可识别。"""
    raw = "|".join([
        str(evt.get("event_name", "")),
        str(evt.get("event_attrs", "")),
        str(evt.get("trigger_condition", "")),
        str(evt.get("occurred_at", "")),
    ])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _parse_occurred(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)


def bucket_for(occurred_at: str, window_size_seconds: int) -> tuple[str, str, int]:
    """按窗口大小对齐 epoch 计算固定时间桶，重放与实时共用同一套时间语义。"""
    ts = int(_parse_occurred(occurred_at).timestamp())
    size = max(int(window_size_seconds), 1)
    bucket_index = ts // size
    start_ts = bucket_index * size
    start = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    end = start + timedelta(seconds=size)
    fmt = "%Y-%m-%dT%H:%M:%S"
    return start.strftime(fmt), end.strftime(fmt), bucket_index


def _business_date(occurred_at: str) -> str:
    return _parse_occurred(occurred_at).strftime("%Y-%m-%d")


def _eligible_windows() -> list[dict[str, Any]]:
    """补算只针对已启用、且消费事件追踪器数据（或全量数据源）的窗口。"""
    return db.fetchall(
        "SELECT * FROM aggregation_windows WHERE status='已启用' "
        "AND (data_source_module='event_tracker' OR data_source_module='all' "
        "OR data_source_module='' OR data_source_module IS NULL) "
        "ORDER BY id ASC"
    )


def _now() -> str:
    return db.now()


# ----------------------------------------------------------------------------
# 事件追踪器：历史批次查询与整组重放
# ----------------------------------------------------------------------------

def list_source_batches() -> list[dict[str, Any]]:
    """列出 events 表中现存的历史事件批次（含实时摄入批次与重放批次）。"""
    rows = db.fetchall(
        "SELECT batch_id, COUNT(*) AS event_count, "
        "MIN(occurred_at) AS first_occurred_at, MAX(occurred_at) AS last_occurred_at, "
        "MIN(ingested_at) AS ingested_at "
        "FROM events WHERE batch_id IS NOT NULL AND batch_id != '' "
        "GROUP BY batch_id ORDER BY MIN(occurred_at) DESC, batch_id DESC"
    )
    result = []
    for row in rows:
        result.append({
            **row,
            "business_date": _business_date(row["first_occurred_at"]),
            "is_replay_batch": str(row["batch_id"]).startswith("REPLAY_"),
        })
    return result


def list_replay_groups(status: str | None = None) -> list[dict[str, Any]]:
    """列出回放组，并附带每个组的来源批次处理情况。"""
    if status:
        groups = db.fetchall(
            "SELECT * FROM replay_groups WHERE status=? ORDER BY id DESC", [status])
    else:
        groups = db.fetchall("SELECT * FROM replay_groups ORDER BY id DESC")
    for group in groups:
        group["sources"] = db.fetchall(
            "SELECT * FROM replay_group_sources WHERE replay_group_id=? ORDER BY id ASC",
            [group["id"]],
        )
    return groups


def get_replay_group(group_id: int) -> dict[str, Any] | None:
    group = db.fetchone("SELECT * FROM replay_groups WHERE id=?", [group_id])
    if group is None:
        return None
    group["sources"] = db.fetchall(
        "SELECT * FROM replay_group_sources WHERE replay_group_id=? ORDER BY id ASC",
        [group_id],
    )
    group["replay_events"] = db.fetchall(
        "SELECT id, replay_group_id, source_id, replay_event_id, replay_batch_id, "
        "source_batch_id, event_name, occurred_at, fingerprint, accept_status "
        "FROM replay_events WHERE replay_group_id=? ORDER BY id ASC",
        [group_id],
    )
    group["affected_windows"] = _affected_windows_for_group(group_id)
    return group


def _affected_windows_for_group(group_id: int) -> list[dict[str, Any]]:
    rows = db.fetchall(
        "SELECT window_id, window_start, window_end, status, "
        "SUM(source_record_count) AS source_records, COUNT(*) AS dimension_groups "
        "FROM agg_correction_results WHERE replay_group_id=? "
        "GROUP BY window_id, window_start, window_end, status ORDER BY window_id, window_start",
        [group_id],
    )
    windows_by_id = {w["id"]: w for w in db.fetchall("SELECT id, window_name FROM aggregation_windows")}
    for row in rows:
        row["window_name"] = windows_by_id.get(row["window_id"], {}).get("window_name", "")
    return rows


def submit_replay_group(
    source_batch_ids: list[str], group_name: str = "", note: str = "",
) -> dict[str, Any]:
    """整组重放：多选历史批次一次提交，立即处理可读批次，缺失批次留待续办。

    幂等：相同来源批次集合的回放组重复提交不会多计（返回已存在组且不重复处理）。
    """
    batch_ids = [b for b in dict.fromkeys(str(b).strip() for b in source_batch_ids) if b]
    if not batch_ids:
        return {"ok": False, "message": "至少选择一个历史事件批次"}

    replay_named = [b for b in batch_ids if b.startswith("REPLAY_")]
    if replay_named:
        return {"ok": False, "message": f"重放批次不能再次作为重放来源：{', '.join(replay_named)}"}

    # 幂等：同来源集合的组已存在则直接返回当前办理结果
    existing = _find_group_by_batches(batch_ids)
    if existing is not None:
        detail = get_replay_group(existing["id"])
        return {
            "ok": True,
            "message": f"回放组 {existing['replay_group_code']} 已存在，重复提交未重复计入",
            "idempotent": True,
            "group": detail,
        }

    code = f"RG_{int(datetime.now(timezone.utc).timestamp() * 1000)}"
    group_id = db.insert("replay_groups", {
        "replay_group_code": code,
        "group_name": group_name or f"整组重放-{len(batch_ids)}个批次",
        "status": GROUP_PROCESSING,
        "note": note,
        "updated_at": _now(),
    })

    for batch_id in batch_ids:
        sample = db.fetchone(
            "SELECT MIN(occurred_at) AS first_at FROM events WHERE batch_id=?", [batch_id])
        biz_date = _business_date(sample["first_at"]) if sample and sample["first_at"] else ""
        db.insert("replay_group_sources", {
            "replay_group_id": group_id,
            "source_batch_id": batch_id,
            "business_date": biz_date,
            "status": SOURCE_PENDING,
            "updated_at": _now(),
        })

    summary = _process_group_sources(group_id)
    return {
        "ok": True,
        "message": summary["message"],
        "idempotent": False,
        "group": get_replay_group(group_id),
        "summary": summary,
    }


def _find_group_by_batches(batch_ids: list[str]) -> dict[str, Any] | None:
    """查找来源批次集合完全一致的回放组（顺序无关）。"""
    target = set(batch_ids)
    groups = db.fetchall("SELECT id FROM replay_groups")
    for g in groups:
        rows = db.fetchall(
            "SELECT source_batch_id FROM replay_group_sources WHERE replay_group_id=?",
            [g["id"]],
        )
        if {r["source_batch_id"] for r in rows} == target:
            return db.fetchone("SELECT * FROM replay_groups WHERE id=?", [g["id"]])
    return None


def resume_replay_group(group_id: int) -> dict[str, Any]:
    """续办：恢复缺失批次样本后继续办理，已计入的贡献不会再加一次。"""
    group = db.fetchone("SELECT * FROM replay_groups WHERE id=?", [group_id])
    if group is None:
        return {"ok": False, "message": f"回放组 {group_id} 不存在"}
    summary = _process_group_sources(group_id)
    return {"ok": True, "message": summary["message"], "group": get_replay_group(group_id), "summary": summary}


def _process_group_sources(group_id: int) -> dict[str, Any]:
    """处理组内所有待读取/缺失的来源批次，随后修正受影响窗口并刷新组状态。"""
    sources = db.fetchall(
        "SELECT * FROM replay_group_sources WHERE replay_group_id=? ORDER BY id ASC",
        [group_id],
    )
    affected: set[tuple[int, str]] = set()
    totals = {"read": 0, "missing": 0, "accepted": 0, "duplicated": 0, "events": 0}

    for source in sources:
        if source["status"] == SOURCE_READ:
            continue
        result = _process_source(group_id, source)
        totals["events"] += result["event_total"]
        totals["accepted"] += result["accepted"]
        totals["duplicated"] += result["duplicated"]
        if result["status"] == SOURCE_MISSING:
            totals["missing"] += 1
        else:
            totals["read"] += 1
            affected.update(result["affected_buckets"])

    if affected:
        _recompute_buckets(affected)
    _refresh_group_status(group_id)

    group = db.fetchone("SELECT status FROM replay_groups WHERE id=?", [group_id])
    if group["status"] == GROUP_DONE:
        message = "组内批次全部读取并完成窗口补算"
    else:
        message = f"已处理可读批次，{totals['missing']} 个批次样本缺失，相关窗口保持待验明"
    return {**totals, "message": message, "affected_bucket_count": len(affected)}


def _process_source(
    group_id: int, source: dict[str, Any],
) -> dict[str, Any]:
    """读取单个原始批次：复制事件与发生时间为重放批次，并登记窗口贡献台账。"""
    batch_id = source["source_batch_id"]
    originals = db.fetchall(
        "SELECT * FROM events WHERE batch_id=? ORDER BY id ASC", [batch_id])

    if not originals:
        db.update("replay_group_sources", {
            "status": SOURCE_MISSING,
            "message": "批次样本暂时无法读取",
            "updated_at": _now(),
        }, "id=?", [source["id"]])
        return {"status": SOURCE_MISSING, "event_total": 0, "accepted": 0,
                "duplicated": 0, "affected_buckets": set()}

    group = db.fetchone("SELECT * FROM replay_groups WHERE id=?", [group_id])
    replay_batch_id = f"REPLAY_{group['replay_group_code']}_{batch_id}"
    windows = _eligible_windows()

    # 先把相关时间桶的实时历史事件补登 LIVE 台账，重放时才能识别“已经计过”的事件
    bucket_keys: set[tuple[int, str, str]] = set()
    for original in originals:
        for window in windows:
            w_start, w_end, _ = bucket_for(original["occurred_at"], window["window_size_seconds"])
            bucket_keys.add((window["id"], w_start, w_end))
    windows_by_id = {w["id"]: w for w in windows}
    for win_id, w_start, w_end in bucket_keys:
        _register_live_baseline(windows_by_id[win_id], w_start, w_end)

    accepted = 0
    duplicated = 0
    affected: set[tuple[int, str]] = set()

    for original in originals:
        fp = event_fingerprint(original)
        # 重放副本写入 events 主表（保留原始 occurred_at），重放批次可被既有批次接口查询
        replay_event_id = db.insert("events", {
            "event_name": original["event_name"],
            "event_attrs": original["event_attrs"],
            "trigger_condition": original.get("trigger_condition", ""),
            "source_module": original.get("source_module", "event_tracker"),
            "occurred_at": original["occurred_at"],
            "batch_id": replay_batch_id,
            "payload_size": original.get("payload_size", 0),
        })

        contributed_any = False
        for window in windows:
            w_start, w_end, _ = bucket_for(original["occurred_at"], window["window_size_seconds"])
            inserted = _insert_contribution({
                "window_id": window["id"],
                "window_start": w_start,
                "window_end": w_end,
                "event_fingerprint": fp,
                "origin_type": ORIGIN_REPLAY,
                "event_id": replay_event_id,
                "source_event_id": original["id"],
                "replay_group_id": group_id,
                "source_batch_id": batch_id,
            })
            if inserted:
                contributed_any = True
                affected.add((window["id"], w_start))

        accept_status = ACCEPT_OK if contributed_any else ACCEPT_DUP
        if contributed_any:
            accepted += 1
        else:
            duplicated += 1

        db.insert("replay_events", {
            "replay_group_id": group_id,
            "source_id": original["id"],
            "replay_event_id": replay_event_id,
            "replay_batch_id": replay_batch_id,
            "source_batch_id": batch_id,
            "event_name": original["event_name"],
            "event_attrs": original["event_attrs"],
            "occurred_at": original["occurred_at"],
            "fingerprint": fp,
            "accept_status": accept_status,
        })

    db.update("replay_group_sources", {
        "status": SOURCE_READ,
        "event_total": len(originals),
        "event_accepted": accepted,
        "event_duplicated": duplicated,
        "message": f"重放批次 {replay_batch_id}",
        "updated_at": _now(),
    }, "id=?", [source["id"]])

    return {"status": SOURCE_READ, "event_total": len(originals), "accepted": accepted,
            "duplicated": duplicated, "affected_buckets": affected}


def _insert_contribution(row: dict[str, Any]) -> bool:
    """台账登记。UNIQUE(window_id, window_start, fingerprint) 保证同事件同窗口只贡献一次。"""
    cursor = db.execute(
        "INSERT OR IGNORE INTO agg_correction_contributions "
        "(window_id, window_start, window_end, event_fingerprint, origin_type, event_id, "
        "source_event_id, replay_group_id, source_batch_id) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (row["window_id"], row["window_start"], row["window_end"], row["event_fingerprint"],
         row["origin_type"], row["event_id"], row["source_event_id"],
         row["replay_group_id"], row["source_batch_id"]),
    )
    return cursor.rowcount > 0


# ----------------------------------------------------------------------------
# 实时聚合器：窗口汇总修正
# ----------------------------------------------------------------------------

def _register_live_baseline(window: dict[str, Any], window_start: str, window_end: str) -> int:
    """把实时链路已采集的历史事件登记为 LIVE 台账（已存在的重放指纹不会被覆盖）。

    排除两类记录：重放副本（REPLAY_ 批次）与已登记为回放来源的历史批次——
    后者的贡献只能经由重放接受流程进入台账，避免历史样本被当作实时事件再计一次。
    """
    events = db.fetchall(
        "SELECT * FROM events WHERE occurred_at >= ? AND occurred_at < ? "
        "AND (batch_id IS NULL OR ("
        "batch_id NOT LIKE 'REPLAY_%' "
        "AND batch_id NOT IN (SELECT source_batch_id FROM replay_group_sources)"
        ")) ORDER BY id ASC",
        [window_start, window_end],
    )
    registered = 0
    for evt in events:
        inserted = _insert_contribution({
            "window_id": window["id"],
            "window_start": window_start,
            "window_end": window_end,
            "event_fingerprint": event_fingerprint(evt),
            "origin_type": ORIGIN_LIVE,
            "event_id": evt["id"],
            "source_event_id": evt["id"],
            "replay_group_id": None,
            "source_batch_id": evt.get("batch_id"),
        })
        if inserted:
            registered += 1
    return registered


def _recompute_buckets(buckets: set[tuple[int, str]]) -> None:
    """对受影响的窗口时间桶执行补算：重建维度分组、重算聚合、刷新状态与溯源链。"""
    windows = {w["id"]: w for w in _eligible_windows()}
    for window_id, window_start in sorted(buckets):
        window = windows.get(window_id)
        if window is None:
            continue
        bucket_row = db.fetchone(
            "SELECT MIN(window_end) AS window_end FROM agg_correction_contributions "
            "WHERE window_id=? AND window_start=?",
            [window_id, window_start],
        )
        window_end = bucket_row["window_end"] if bucket_row else None
        if not window_end:
            continue

        # 先把该桶实时链路的历史事件补登台账（INSERT OR IGNORE 不影响重放贡献）
        _register_live_baseline(window, window_start, window_end)

        ledger = db.fetchall(
            "SELECT * FROM agg_correction_contributions "
            "WHERE window_id=? AND window_start=? ORDER BY id ASC",
            [window_id, window_start],
        )
        if not ledger:
            continue

        # 该桶是否仍有“未验明完整”的回放来源（组内存在样本缺失批次）
        unverified = db.fetchone(
            "SELECT 1 FROM replay_group_sources s "
            "JOIN agg_correction_contributions c ON c.replay_group_id = s.replay_group_id "
            "WHERE c.window_id=? AND c.window_start=? AND s.status != ? LIMIT 1",
            [window_id, window_start, SOURCE_READ],
        )
        correction_status = CORRECTION_UNVERIFIED if unverified else CORRECTION_DONE

        # 关联实际接受的原事件：回表 events 取属性参与维度分组与聚合
        event_ids = [c["event_id"] for c in ledger if c["event_id"] is not None]
        events_by_id: dict[int, dict[str, Any]] = {}
        if event_ids:
            placeholders = ",".join("?" for _ in event_ids)
            for evt in db.fetchall(f"SELECT * FROM events WHERE id IN ({placeholders})", event_ids):
                events_by_id[evt["id"]] = evt

        try:
            dims = json.loads(window["dimensions"]) if isinstance(window["dimensions"], str) else window["dimensions"]
        except (json.JSONDecodeError, TypeError):
            dims = []

        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for entry in ledger:
            evt = events_by_id.get(entry["event_id"])
            if evt is None:
                continue
            if dims:
                try:
                    attrs = json.loads(evt.get("event_attrs", "{}")) if isinstance(evt.get("event_attrs"), str) else evt.get("event_attrs", {})
                except (json.JSONDecodeError, TypeError):
                    attrs = {}
                dim_key = "|".join(str(attrs.get(d, "?")) for d in dims)
            else:
                dim_key = "__all__"
            groups[dim_key].append(evt)

        for dim_key, group_events in groups.items():
            value = _aggregate_value(window.get("agg_function", "COUNT"), group_events)
            existing = db.fetchone(
                "SELECT * FROM agg_correction_results WHERE window_id=? AND window_start=? AND dimension_key=?",
                [window_id, window_start, dim_key],
            )
            replay_group_ids = sorted({
                c["replay_group_id"] for c in ledger
                if c["origin_type"] == ORIGIN_REPLAY and c["replay_group_id"] is not None
                and events_by_id.get(c["event_id"]) is not None
            })
            # 一个桶可能被多个回放组触及，取最近处理的回放组作为归属组，溯源链保留全部组
            owner_group_id = replay_group_ids[-1] if replay_group_ids else None

            if existing is None:
                version = 1
                correction_id = db.insert("agg_correction_results", {
                    "window_id": window_id,
                    "window_start": window_start,
                    "window_end": window_end,
                    "dimension_key": dim_key,
                    "computed_value": round(value, 4),
                    "event_count": len(group_events),
                    "source_record_count": len(group_events),
                    "replay_group_id": owner_group_id,
                    "computation_version": version,
                    "status": correction_status,
                    "updated_at": _now(),
                })
            else:
                version = int(existing["computation_version"]) + 1
                correction_id = existing["id"]
                db.update("agg_correction_results", {
                    "window_end": window_end,
                    "computed_value": round(value, 4),
                    "event_count": len(group_events),
                    "source_record_count": len(group_events),
                    "replay_group_id": owner_group_id,
                    "computation_version": version,
                    "status": correction_status,
                    "updated_at": _now(),
                }, "id=?", [correction_id])

            _rebuild_correction_sources(correction_id, window_id, window_start, dim_key)


def _rebuild_correction_sources(
    correction_id: int, window_id: int, window_start: str, dim_key: str,
) -> None:
    """重建修正分组 → 原始批次溯源链（按维度分组内实际接受的重放事件统计）。"""
    db.delete("agg_correction_sources", "correction_id=?", [correction_id])
    rows = db.fetchall(
        "SELECT c.replay_group_id, c.source_batch_id, COUNT(*) AS event_count "
        "FROM agg_correction_contributions c "
        "WHERE c.window_id=? AND c.window_start=? AND c.origin_type=? "
        "AND c.replay_group_id IS NOT NULL "
        "GROUP BY c.replay_group_id, c.source_batch_id ORDER BY c.replay_group_id, c.source_batch_id",
        [window_id, window_start, ORIGIN_REPLAY],
    )
    for row in rows:
        db.insert("agg_correction_sources", {
            "correction_id": correction_id,
            "replay_group_id": row["replay_group_id"],
            "source_batch_id": row["source_batch_id"],
            "event_count": row["event_count"],
        })


def _aggregate_value(agg_func: str, group_events: list[dict[str, Any]]) -> float:
    """与 pipeline.compute_aggregation 保持一致的聚合口径。"""
    func = str(agg_func or "COUNT").upper().strip()

    def _amounts() -> list[float]:
        vals = []
        for e in group_events:
            try:
                attrs = json.loads(e.get("event_attrs", "{}")) if isinstance(e.get("event_attrs"), str) else e.get("event_attrs", {})
                vals.append(float(attrs.get("amount", 0)))
            except (json.JSONDecodeError, TypeError, ValueError):
                continue
        return vals

    if func == "COUNT":
        return float(len(group_events))
    if func in ("SUM", "总金额", "总量"):
        vals = _amounts()
        return float(sum(vals)) if vals else float(len(group_events))
    if func in ("AVG", "均值"):
        vals = _amounts()
        return float(statistics.mean(vals)) if vals else 0.0
    if func in ("MAX", "最大值"):
        vals = _amounts()
        return float(max(vals)) if vals else 0.0
    if func in ("MIN", "最小值"):
        vals = _amounts()
        return float(min(vals)) if vals else 0.0
    if func == "COUNT_DISTINCT":
        uids = set()
        for e in group_events:
            try:
                attrs = json.loads(e.get("event_attrs", "{}")) if isinstance(e.get("event_attrs"), str) else e.get("event_attrs", {})
                uids.add(attrs.get("user_id", ""))
            except (json.JSONDecodeError, TypeError):
                continue
        return float(len(uids))
    return float(len(group_events))


def _refresh_group_status(group_id: int) -> None:
    """组内所有来源批次均已读取才算完成；仍有缺失批次时保持处理中。"""
    pending = db.count(
        "replay_group_sources",
        "replay_group_id=? AND status != ?",
        [group_id, SOURCE_READ],
    )
    status = GROUP_DONE if pending == 0 else GROUP_PROCESSING
    db.update("replay_groups", {"status": status, "updated_at": _now()}, "id=?", [group_id])

    # 来源补齐后，把该组触及的待验明窗口重新判定状态
    if pending == 0:
        rows = db.fetchall(
            "SELECT DISTINCT window_id, window_start FROM agg_correction_contributions "
            "WHERE replay_group_id=?",
            [group_id],
        )
        if rows:
            _recompute_buckets({(r["window_id"], r["window_start"]) for r in rows})


# ----------------------------------------------------------------------------
# 实时聚合器：补算结果与溯源查询
# ----------------------------------------------------------------------------

def list_corrections(
    window_id: int | None = None,
    status_filter: str | None = None,
    replay_group_id: int | None = None,
) -> list[dict[str, Any]]:
    where = ["1=1"]
    params: list[Any] = []
    if window_id is not None:
        where.append("r.window_id=?")
        params.append(window_id)
    if status_filter:
        where.append("r.status=?")
        params.append(status_filter)
    if replay_group_id is not None:
        where.append("r.replay_group_id=?")
        params.append(replay_group_id)
    rows = db.fetchall(
        "SELECT r.*, w.window_name FROM agg_correction_results r "
        "JOIN aggregation_windows w ON w.id = r.window_id "
        f"WHERE {' AND '.join(where)} ORDER BY r.window_id, r.window_start, r.dimension_key",
        params,
    )
    for row in rows:
        row["correction_sources"] = db.fetchall(
            "SELECT * FROM agg_correction_sources WHERE correction_id=? "
            "ORDER BY replay_group_id, source_batch_id",
            [row["id"]],
        )
    return rows


def get_correction(correction_id: int) -> dict[str, Any] | None:
    row = db.fetchone(
        "SELECT r.*, w.window_name FROM agg_correction_results r "
        "JOIN aggregation_windows w ON w.id = r.window_id WHERE r.id=?",
        [correction_id],
    )
    if row is None:
        return None
    row["correction_sources"] = db.fetchall(
        "SELECT * FROM agg_correction_sources WHERE correction_id=? "
        "ORDER BY replay_group_id, source_batch_id",
        [correction_id],
    )
    return row


def list_correction_sources(
    replay_group_id: int | None = None, source_batch_id: str | None = None,
) -> list[dict[str, Any]]:
    """溯源回查：哪些已修正/待验明分组收到了哪个原始批次。"""
    where = ["1=1"]
    params: list[Any] = []
    if replay_group_id is not None:
        where.append("s.replay_group_id=?")
        params.append(replay_group_id)
    if source_batch_id:
        where.append("s.source_batch_id=?")
        params.append(source_batch_id)
    return db.fetchall(
        "SELECT s.*, r.window_id, w.window_name, r.window_start, r.window_end, "
        "r.dimension_key, r.status AS correction_status, r.source_record_count, "
        "g.replay_group_code, g.group_name "
        "FROM agg_correction_sources s "
        "JOIN agg_correction_results r ON r.id = s.correction_id "
        "JOIN aggregation_windows w ON w.id = r.window_id "
        "JOIN replay_groups g ON g.id = s.replay_group_id "
        f"WHERE {' AND '.join(where)} "
        "ORDER BY g.id, s.source_batch_id, r.window_id, r.window_start",
        params,
    )


def window_source_records(window_id: int, window_start: str) -> list[dict[str, Any]]:
    """回查某窗口时间桶的源记录台账（LIVE + 各回放组来源）。"""
    return db.fetchall(
        "SELECT c.*, e.event_name, e.occurred_at, e.batch_id AS event_batch_id "
        "FROM agg_correction_contributions c "
        "LEFT JOIN events e ON e.id = c.event_id "
        "WHERE c.window_id=? AND c.window_start=? ORDER BY c.id ASC",
        [window_id, window_start],
    )
