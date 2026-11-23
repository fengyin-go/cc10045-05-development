"""实时聚合器领域服务：管理聚合窗口配置并驱动真实聚合计算。

真实数据流：接收事件定义 → 按窗口维度分组 → 执行 COUNT/SUM/AVG/MAX/MIN 等聚合函数
→ 结果写入 aggregation_results 表 → 供下游指标存储和仪表盘消费。

窗口类型支持 tumbling（滚动）、sliding（滑动）、session（会话）三种，
每种类型有独立的时间语义和结果合并策略。
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from app.database import db
from app.pipeline import compute_aggregation
from app.services.base import BaseModuleService


class RealtimeAggService(BaseModuleService):
    MODULE = "realtime_agg"
    ENTITY = "聚合窗口"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["窗口名称", "数据源", "聚合维度", "聚合函数", "窗口大小"]
    REQUIRED_FIELDS = ["窗口名称", "数据源", "聚合维度"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用聚合": "已启用", "停用聚合": "已停用"}
    NEGATIVE_ACTIONS = ["停用聚合"]

    # ================================================================
    # 扩展查询
    # ================================================================

    def list_windows(
        self, status: str | None = None, data_source: str | None = None,
        page: int = 1, size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        """列出聚合窗口配置，支持按数据源筛选。"""
        where = "1=1"
        params: list[Any] = []
        if status:
            where += " AND status=?"
            params.append(status)
        if data_source:
            where += " AND data_source_module=?"
            params.append(data_source)

        total = db.count("aggregation_windows", where, params)
        rows = db.fetchall(
            f"SELECT * FROM aggregation_windows WHERE {where} ORDER BY id DESC LIMIT ? OFFSET ?",
            params + [size, (page - 1) * size],
        )
        return _sqlite_rows_to_dict(rows), total

    def results_by_window(
        self, window_id: int, limit: int = 50,
    ) -> list[dict[str, Any]]:
        """查询指定窗口的聚合结果。按窗口起始时间倒序。"""
        rows = db.fetchall(
            "SELECT * FROM aggregation_results WHERE window_id=? ORDER BY window_start DESC LIMIT ?",
            [window_id, limit],
        )
        return _sqlite_rows_to_dict(rows)

    def latest_value(
        self, window_id: int, dimension_key: str = "__all__",
    ) -> dict[str, Any] | None:
        """获取指定窗口和维度的最新聚合值。"""
        return db.fetchone(
            "SELECT * FROM aggregation_results WHERE window_id=? AND dimension_key=? "
            "ORDER BY window_start DESC LIMIT 1",
            [window_id, dimension_key],
        )

    # ================================================================
    # 聚合执行
    # ================================================================

    def run_aggregation(self, window_id: int) -> dict[str, Any]:
        """对指定窗口执行实时聚合计算。

        1. 从 aggregation_windows 加载窗口配置
        2. 调用 pipeline.compute_aggregation 执行计算
        3. 返回本次计算的结果摘要
        """
        window = db.fetchone("SELECT * FROM aggregation_windows WHERE id=?", [window_id])
        if window is None:
            return {"ok": False, "message": f"窗口 {window_id} 不存在", "results": []}

        if window["status"] != "已启用":
            return {"ok": False, "message": f"窗口 {window['window_name']} 未启用，当前状态: {window['status']}", "results": []}

        results = compute_aggregation(dict(window))

        return {
            "ok": True,
            "message": f"窗口 {window['window_name']} 完成聚合，产出 {len(results)} 条结果",
            "results": results,
            "window_name": window["window_name"],
            "window_type": window["window_type"],
            "total_value": sum(r["computed_value"] for r in results),
        }

    def run_all_active(self) -> dict[str, Any]:
        """对所有已启用窗口执行聚合。"""
        windows = db.fetchall("SELECT * FROM aggregation_windows WHERE status='已启用'")
        summary = []
        total_results = 0
        for w in windows:
            r = self.run_aggregation(w["id"])
            summary.append({"window_name": w["window_name"], "result_count": len(r["results"])})
            total_results += len(r["results"])
        return {"ok": True, "windows_processed": len(windows), "total_results": total_results, "summary": summary}

    # ================================================================
    # 重放补算：关联实际接受的原事件与窗口，修正窗口汇总并支持溯源
    # ================================================================

    def list_corrections(
        self, window_id: int | None = None, status_filter: str | None = None,
        replay_group_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """列出窗口补算结果，可按窗口、补算状态、回放组筛选。"""
        from app import replay
        return replay.list_corrections(
            window_id=window_id, status_filter=status_filter, replay_group_id=replay_group_id)

    def get_correction(self, correction_id: int) -> dict[str, Any] | None:
        """查询单条补算分组及其收到的原始批次溯源链。"""
        from app import replay
        return replay.get_correction(correction_id)

    def list_correction_sources(
        self, replay_group_id: int | None = None, source_batch_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """回查哪些补算分组收到了哪个原始批次（同组不同日期/来源可核）。"""
        from app import replay
        return replay.list_correction_sources(
            replay_group_id=replay_group_id, source_batch_id=source_batch_id)

    def window_source_records(self, window_id: int, window_start: str) -> list[dict[str, Any]]:
        """回查窗口时间桶的源记录台账，核对同一事件是否只贡献一次。"""
        from app import replay
        return replay.window_source_records(window_id, window_start)

    def time_range_analysis(
        self, window_id: int, start_time: str, end_time: str,
    ) -> dict[str, Any]:
        """对指定时间范围的聚合结果做趋势分析。"""
        rows = db.fetchall(
            "SELECT * FROM aggregation_results WHERE window_id=? AND window_start>=? AND window_start<=? "
            "ORDER BY window_start ASC",
            [window_id, start_time, end_time],
        )
        if not rows:
            return {"trend": "no_data", "points": []}

        values = [r["computed_value"] for r in rows]
        points = [{"time": r["window_start"], "value": r["computed_value"], "count": r["event_count"]} for r in rows]

        import statistics
        mean_v = statistics.mean(values)
        std_v = statistics.stdev(values) if len(values) > 1 else 0
        trend = "stable"
        if len(values) >= 2:
            if values[0] < values[-1] * 0.7:
                trend = "upward"
            elif values[0] > values[-1] * 1.3:
                trend = "downward"

        return {
            "trend": trend,
            "mean": round(mean_v, 4),
            "std": round(std_v, 4),
            "min": round(min(values), 4),
            "max": round(max(values), 4),
            "points": points,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        enabled = db.count("aggregation_windows", "status='已启用'")
        total_events = db.count("events")
        total_results = db.count("aggregation_results")
        snapshot["enabled_windows"] = enabled
        snapshot["total_ingested_events"] = total_events
        snapshot["total_agg_results"] = total_results
        snapshot["latest_batch"] = db.fetchone("SELECT batch_id, MAX(ingested_at) as ts FROM events")
        return snapshot


def _sqlite_rows_to_dict(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return rows