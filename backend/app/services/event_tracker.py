"""事件追踪器领域服务：定义和管理业务事件，驱动事件采集管道。

真实数据流：管理事件定义（名称/属性/触发条件/关联指标）
→ 调用 pipeline.ingest_events 批量摄入模拟事件 → events 表
→ 下游 reatime_agg/funnel_analyzer/retention_analysis 消费事件数据。
"""
from __future__ import annotations

import json
from typing import Any

from app.database import db
from app.pipeline import ingest_events
from app.services.base import BaseModuleService


class EventTrackerService(BaseModuleService):
    MODULE = "event_tracker"
    ENTITY = "事件定义"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["事件名称", "事件属性", "触发条件", "关联指标", "最近触达"]
    REQUIRED_FIELDS = ["事件名称", "事件属性", "触发条件"]
    STATUS_ORDER = ["草稿", "已启用", "已冻结"]
    ACTION_RULES = {"启用事件": "已启用", "冻结事件": "已冻结"}
    NEGATIVE_ACTIONS = []

    # ================================================================
    # 事件采集
    # ================================================================

    def ingest_all_enabled(self, batch_id: str | None = None) -> dict[str, Any]:
        """对所有已启用的事件定义执行批量摄入。"""
        enabled = db.fetchall(
            "SELECT * FROM events WHERE 1=0"
        )
        event_defs = db.fetchall(
            "SELECT * FROM aggregation_windows WHERE 1=0"
        )

        enabled_events = self.list_entries(status="已启用", page=1, size=1000)[0]

        if not enabled_events:
            return {"ok": False, "message": "没有已启用的事件定义", "ingested": 0}

        count = ingest_events(enabled_events, batch_id)
        return {"ok": True, "ingested": count, "events_processed": len(enabled_events)}

    def ingest_single(self, entry_id: int) -> dict[str, Any]:
        """对单个事件定义摄入数据。先确认事件已启用。"""
        entry = self.get_entry(entry_id)
        if entry is None:
            return {"ok": False, "message": f"事件定义 {entry_id} 不存在"}

        if entry.get("status") != "已启用":
            return {"ok": False, "message": f"事件 {entry.get('事件名称')} 未启用"}

        count = ingest_events([entry])
        return {"ok": True, "ingested": count, "event_name": entry.get("事件名称")}

    # ================================================================
    # 事件分析
    # ================================================================

    def event_distribution(self, event_name: str, hours: int = 24) -> dict[str, Any]:
        """分析指定事件在最近N小时的分布（按小时）。"""
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        start = (now - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%S")

        rows = db.fetchall(
            "SELECT strftime('%H', occurred_at) as hour_slot, COUNT(*) as cnt "
            "FROM events WHERE event_name LIKE ? AND occurred_at >= ? "
            "GROUP BY hour_slot ORDER BY hour_slot",
            [f"%{event_name}%", start],
        )

        distribution = {r["hour_slot"]: r["cnt"] for r in rows}
        total = sum(distribution.values())
        peak_hour = max(distribution, key=distribution.get) if distribution else None

        return {
            "event_name": event_name,
            "hours": hours,
            "total": total,
            "distribution": distribution,
            "peak_hour": peak_hour,
        }

    def attribute_breakdown(self, event_name: str, attr_key: str, top_n: int = 10) -> dict[str, Any]:
        """按指定属性键对事件进行分组统计。"""
        rows = db.fetchall(
            "SELECT event_attrs FROM events WHERE event_name LIKE ?",
            [f"%{event_name}%"],
        )

        from collections import Counter
        counter: Counter[str] = Counter()
        for r in rows:
            try:
                attrs = json.loads(r["event_attrs"]) if isinstance(r["event_attrs"], str) else r.get("event_attrs", {})
                val = str(attrs.get(attr_key, "unknown"))
                counter[val] += 1
            except (json.JSONDecodeError, TypeError):
                counter["parse_error"] += 1

        top = counter.most_common(top_n)
        return {
            "event_name": event_name,
            "attribute": attr_key,
            "total_events": sum(counter.values()),
            "unique_values": len(counter),
            "top_values": [{"value": v, "count": c} for v, c in top],
        }

    def recent_events(self, event_name: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        """获取最近的事件记录。"""
        if event_name:
            rows = db.fetchall(
                "SELECT * FROM events WHERE event_name LIKE ? ORDER BY ingested_at DESC LIMIT ?",
                [f"%{event_name}%", limit],
            )
        else:
            rows = db.fetchall(
                "SELECT * FROM events ORDER BY ingested_at DESC LIMIT ?",
                [limit],
            )
        return rows

    def batch_summary(self, batch_id: str) -> dict[str, Any]:
        """获取指定批次的事件摘要。"""
        rows = db.fetchall(
            "SELECT event_name, COUNT(*) as cnt, MIN(occurred_at) as first_at, MAX(occurred_at) as last_at "
            "FROM events WHERE batch_id=? GROUP BY event_name",
            [batch_id],
        )
        total = sum(r["cnt"] for r in rows)
        return {
            "batch_id": batch_id,
            "total_events": total,
            "event_types": len(rows),
            "breakdown": [{"event_name": r["event_name"], "count": r["cnt"],
                          "first": r["first_at"], "last": r["last_at"]} for r in rows],
        }

    # ================================================================
    # 整组重放：多选历史批次一次提交，产出重放批次并驱动窗口补算
    # ================================================================

    def list_source_batches(self) -> list[dict[str, Any]]:
        """列出可供重放选择的历史事件批次。"""
        from app import replay
        return replay.list_source_batches()

    def submit_replay_group(
        self, source_batch_ids: list[str], group_name: str = "", note: str = "",
    ) -> dict[str, Any]:
        """整组重放提交：同组批次可跨日期，样本缺失的批次留待续办。"""
        from app import replay
        return replay.submit_replay_group(source_batch_ids, group_name=group_name, note=note)

    def resume_replay_group(self, group_id: int) -> dict[str, Any]:
        """恢复缺失批次样本后续办，已计入的贡献不会重复计入。"""
        from app import replay
        return replay.resume_replay_group(group_id)

    def list_replay_groups(self, status: str | None = None) -> list[dict[str, Any]]:
        from app import replay
        return replay.list_replay_groups(status=status)

    def get_replay_group(self, group_id: int) -> dict[str, Any] | None:
        from app import replay
        return replay.get_replay_group(group_id)

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        total_events = db.count("events")
        enabled_count = db.count("events", "1=1")
        snapshot["total_events_ingested"] = total_events
        snapshot["unique_event_names"] = len(db.fetchall(
            "SELECT DISTINCT event_name FROM events"))
        return snapshot