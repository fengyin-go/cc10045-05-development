"""数据分析管道路由：事件采集、聚合执行、指标快照、分群计算等核心操作入口。"""
from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.database import db
from app.pipeline import (
    check_alert, compute_aggregation, compute_cohort_comparison,
    compute_forecast, compute_funnel, compute_metric_snapshot,
    compute_retention, compute_segment, generate_report,
    ingest_events, precompute_cube, seed_real_data,
)
from app.services import service_for

router = APIRouter(prefix="/api/pipeline", tags=["分析管线"])


class ReplayGroupPayload(BaseModel):
    source_batch_ids: list[str] = Field(default_factory=list)
    group_name: str = ""
    note: str = ""


@router.post("/seed")
def api_seed_real_data() -> dict:
    """初始化全部种子数据：事件、指标、聚合窗口、分群、漏斗等。"""
    counts = seed_real_data()
    return {"ok": True, "counts": counts}


@router.post("/events/ingest")
def api_ingest_events() -> dict:
    """对所有已启用的事件定义批量摄入模拟事件。"""
    svc = service_for("event_tracker")
    result = svc.ingest_all_enabled()
    return result


@router.post("/aggregation/run-all")
def api_run_all_aggregations() -> dict:
    """对所有已启用聚合窗口执行计算。"""
    svc = service_for("realtime_agg")
    result = svc.run_all_active()
    return result


@router.post("/aggregation/{window_id}/run")
def api_run_aggregation(window_id: int) -> dict:
    """对指定聚合窗口执行单次计算。"""
    svc = service_for("realtime_agg")
    return svc.run_aggregation(window_id)


@router.post("/metrics/compute-all")
def api_compute_all_metrics() -> dict:
    """对所有指标执行快照计算。"""
    svc = service_for("metric_store")
    return svc.compute_all()


@router.post("/metrics/{metric_id}/snapshot")
def api_compute_snapshot(metric_id: int) -> dict:
    """对指定指标执行快照计算。"""
    svc = service_for("metric_store")
    return svc.compute_snapshot(metric_id)


@router.post("/segments/{segment_id}/compute")
def api_compute_segment(segment_id: int) -> dict:
    """对指定分群执行计算。"""
    svc = service_for("segment_engine")
    return svc.run_computation(segment_id)


@router.post("/funnels/{funnel_id}/compute")
def api_compute_funnel(funnel_id: int) -> dict:
    """对指定漏斗执行分析计算。"""
    svc = service_for("funnel_analyzer")
    return svc.run_computation(funnel_id)


@router.post("/retention/{task_id}/compute")
def api_compute_retention(task_id: int) -> dict:
    """对指定留存任务执行计算。"""
    svc = service_for("retention_analysis")
    return svc.run_computation(task_id)


@router.post("/forecast/{task_id}/compute")
def api_compute_forecast(task_id: int) -> dict:
    """对指定预测任务执行计算。"""
    svc = service_for("forecast_model")
    return svc.run_forecast(task_id)


@router.post("/comparison/{comparison_id}/compute")
def api_compute_comparison(comparison_id: int) -> dict:
    """对指定分群对比执行计算。"""
    svc = service_for("cohort_compare")
    return svc.run_comparison(comparison_id)


@router.post("/cube/{cube_id}/precompute")
def api_precompute_cube(cube_id: int) -> dict:
    """对指定Cube执行预计算。"""
    svc = service_for("cube_builder")
    return svc.run_precomputation(cube_id)


@router.post("/report/{report_id}/generate")
def api_generate_report(report_id: int) -> dict:
    """对指定报告模板执行生成。"""
    svc = service_for("data_report")
    return svc.generate(report_id)


@router.post("/alerts/check-all")
def api_check_all_alerts() -> dict:
    """检查所有已生效告警规则。"""
    svc = service_for("alert_policy")
    return svc.check_all()


@router.get("/analytics/full-pipeline")
def api_full_pipeline() -> dict:
    """执行完整分析链路：事件采集→聚合→指标→分群→漏斗→留存→预测→报告。"""
    steps = {}

    # 1. 事件采集
    svc_events = service_for("event_tracker")
    steps["events"] = svc_events.ingest_all_enabled()

    # 2. 聚合
    svc_agg = service_for("realtime_agg")
    steps["aggregation"] = svc_agg.run_all_active()

    # 3. 指标快照
    svc_metrics = service_for("metric_store")
    steps["metrics"] = svc_metrics.compute_all()

    # 4. 分群
    segments = db.fetchall("SELECT id FROM segments LIMIT 6")
    seg_results = []
    for s in segments:
        r = service_for("segment_engine").run_computation(s["id"])
        seg_results.append(r)
    steps["segments"] = {"processed": len(segments), "results": seg_results}

    # 5. 漏斗
    funnels = db.fetchall("SELECT id FROM funnels LIMIT 3")
    funnel_results = []
    for f in funnels:
        r = service_for("funnel_analyzer").run_computation(f["id"])
        funnel_results.append(r)
    steps["funnels"] = {"processed": len(funnels), "results": funnel_results}

    # 6. 留存
    ret_tasks = db.fetchall("SELECT id FROM retention_tasks LIMIT 3")
    ret_results = []
    for t in ret_tasks:
        r = service_for("retention_analysis").run_computation(t["id"])
        ret_results.append(r)
    steps["retention"] = {"processed": len(ret_tasks), "results": ret_results}

    # 7. 预测
    forecast_tasks = db.fetchall("SELECT id FROM forecast_tasks LIMIT 2")
    fc_results = []
    for t in forecast_tasks:
        r = service_for("forecast_model").run_forecast(t["id"])
        fc_results.append(r)
    steps["forecast"] = {"processed": len(forecast_tasks), "results": fc_results}

    # 8. 报告
    reports = db.fetchall("SELECT id FROM reports LIMIT 2")
    report_results = []
    for rep in reports:
        r = service_for("data_report").generate(rep["id"])
        report_results.append(r)
    steps["reports"] = {"processed": len(reports), "results": report_results}

    return {"ok": True, "pipeline": steps}


# ============================================================================
# 整组重放与窗口补算
# ============================================================================

@router.get("/replay/batches")
def api_replay_source_batches() -> dict:
    """列出可被整组重放选择的历史事件批次。"""
    svc = service_for("event_tracker")
    items = svc.list_source_batches()
    return {"ok": True, "items": items, "total": len(items)}


@router.post("/replay/groups")
def api_submit_replay_group(payload: ReplayGroupPayload) -> dict:
    """值班员多选批次一次提交整组重放（同组批次可跨日期）。"""
    svc = service_for("event_tracker")
    return svc.submit_replay_group(
        payload.source_batch_ids, group_name=payload.group_name, note=payload.note)


@router.get("/replay/groups")
def api_list_replay_groups(status: str | None = None) -> dict:
    """列出回放组及其来源批次办理状态。"""
    svc = service_for("event_tracker")
    items = svc.list_replay_groups(status=status)
    return {"ok": True, "items": items, "total": len(items)}


@router.get("/replay/groups/{group_id}")
def api_get_replay_group(group_id: int) -> dict:
    """回放组详情：来源批次、重放事件接受情况、受影响窗口。"""
    svc = service_for("event_tracker")
    group = svc.get_replay_group(group_id)
    if group is None:
        return {"ok": False, "message": f"回放组 {group_id} 不存在"}
    return {"ok": True, "group": group}


@router.post("/replay/groups/{group_id}/resume")
def api_resume_replay_group(group_id: int) -> dict:
    """缺失批次样本恢复后续办，已计入贡献不会重复计入。"""
    svc = service_for("event_tracker")
    return svc.resume_replay_group(group_id)


@router.get("/aggregation/corrections/sources")
def api_correction_sources(
    replay_group_id: int | None = None, source_batch_id: str | None = None,
) -> dict:
    """溯源回查：补算分组收到了哪个回放组的哪个原始批次。"""
    svc = service_for("realtime_agg")
    items = svc.list_correction_sources(
        replay_group_id=replay_group_id, source_batch_id=source_batch_id)
    return {"ok": True, "items": items, "total": len(items)}


@router.get("/aggregation/corrections")
def api_list_corrections(
    window_id: int | None = None,
    status: str | None = None,
    replay_group_id: int | None = None,
) -> dict:
    """列出窗口补算结果；待验明窗口不会被当作完成补算。"""
    svc = service_for("realtime_agg")
    items = svc.list_corrections(
        window_id=window_id, status_filter=status, replay_group_id=replay_group_id)
    return {"ok": True, "items": items, "total": len(items)}


@router.get("/aggregation/corrections/{correction_id}")
def api_get_correction(correction_id: int) -> dict:
    """单条补算分组详情，含修正值、源记录数与原始批次溯源链。"""
    svc = service_for("realtime_agg")
    item = svc.get_correction(correction_id)
    if item is None:
        return {"ok": False, "message": f"补算记录 {correction_id} 不存在"}
    return {"ok": True, "correction": item}


@router.get("/aggregation/windows/{window_id}/correction-sources")
def api_window_correction_sources(window_id: int, window_start: str) -> dict:
    """回查指定窗口时间桶的源记录台账（LIVE/REPLAY 贡献）。"""
    svc = service_for("realtime_agg")
    items = svc.window_source_records(window_id, window_start)
    return {"ok": True, "items": items, "total": len(items)}