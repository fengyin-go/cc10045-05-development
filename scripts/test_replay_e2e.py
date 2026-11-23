"""整组重放端到端场景验证（与业务数据目录隔离运行）。

场景：同一回放组含周一、周二两个批次；周二样本暂时缺失，先办理周一；
恢复周二后续办，校验幂等、单次贡献、待验明/已修正状态与溯源回查。
运行：CC10045_DB_DIR=<tmp> python -I test_replay_e2e.py
"""
from __future__ import annotations

import os
import sys
import tempfile

_TMP = tempfile.mkdtemp(prefix="replay_e2e_")
os.environ["CC10045_DB_DIR"] = _TMP
sys.path.insert(0, "/workspace/backend")

from app.database import db  # noqa: E402
from app.pipeline import seed_real_data  # noqa: E402
from app import replay  # noqa: E402

failures: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"[{mark}] {name} {detail}")
    if not condition:
        failures.append(name)


# 1. 初始化种子数据
seed_real_data()

# 2. 准备一个确定性的小时窗口（已启用、消费 event_tracker 数据）
window_id = db.insert("aggregation_windows", {
    "window_name": "对账用小时窗口",
    "data_source_module": "event_tracker",
    "dimensions": "[]",
    "agg_function": "COUNT",
    "window_type": "tumbling",
    "window_size_seconds": 3600,
    "status": "已启用",
})
# 再建一个不同窗口大小的窗口，验证“同一事件同一窗口一次、跨窗口各自贡献”
window_b_id = db.insert("aggregation_windows", {
    "window_name": "对账用日窗口",
    "data_source_module": "event_tracker",
    "dimensions": "[]",
    "agg_function": "COUNT",
    "window_type": "tumbling",
    "window_size_seconds": 86400,
    "status": "已启用",
})

# 选远离种子数据时间范围（最近24小时）的周一/周二，保证测试桶内只有本场景事件
MON_HOUR = "2026-12-07T10:00:"  # 周一 10 点档
TUE_HOUR = "2026-12-08T11:00:"  # 周二 11 点档


def insert_event(name: str, occurred: str, batch: str, amount: float = 10.0) -> int:
    return db.insert("events", {
        "event_name": name,
        "event_attrs": f'{{"user_id": "u_x", "amount": {amount}}}',
        "trigger_condition": "测试触发",
        "source_module": "event_tracker",
        "occurred_at": occurred,
        "batch_id": batch,
        "payload_size": 128,
    })


# 3. 周一实时链路已采集 3 个事件（原始已计贡献）
live_mon = [
    ("e1", f"{MON_HOUR}05"),
    ("e2", f"{MON_HOUR}20"),
    ("e3", f"{MON_HOUR}35"),
]
for name, ts in live_mon:
    insert_event(name, ts, "LIVE_MON")

# 4. 周一历史批次：含 1 个已计过的事件（与 e1 同指纹）+ 2 个漏采事件
mon_source = [
    ("e1", f"{MON_HOUR}05"),   # 重复
    ("e4", f"{MON_HOUR}40"),   # 漏采
    ("e5", f"{MON_HOUR}50"),   # 漏采
]
for name, ts in mon_source:
    insert_event(name, ts, "BATCH_MON")

# 5. 周二历史批次（稍后删除模拟样本缺失）
tue_source = [
    ("t1", f"{TUE_HOUR}05"),
    ("t2", f"{TUE_HOUR}25"),
]
for name, ts in tue_source:
    insert_event(name, ts, "BATCH_TUE")
tue_rows = db.fetchall("SELECT * FROM events WHERE batch_id='BATCH_TUE' ORDER BY id")
check("周二批次初始有 2 条样本", len(tue_rows) == 2)
db.delete("events", "batch_id='BATCH_TUE'")

# 6. 值班员多选两个批次一次提交整组重放
submit = replay.submit_replay_group(
    ["BATCH_MON", "BATCH_TUE"], group_name="周一周二跨日补算组")
check("首次提交成功", submit["ok"] and not submit.get("idempotent"))
group = submit["group"]
gid = group["id"]
mon_src = next(s for s in group["sources"] if s["source_batch_id"] == "BATCH_MON")
tue_src = next(s for s in group["sources"] if s["source_batch_id"] == "BATCH_TUE")
check("周一批次已读取，接受 2 条、跳过 1 条重复",
      mon_src["status"] == replay.SOURCE_READ
      and mon_src["event_accepted"] == 2 and mon_src["event_duplicated"] == 1,
      str({k: mon_src[k] for k in ("status", "event_accepted", "event_duplicated")}))
check("周二批次标记样本缺失", tue_src["status"] == replay.SOURCE_MISSING)
check("回放组保持处理中", group["status"] == replay.GROUP_PROCESSING)

replay_mon = [e for e in group["replay_events"] if e["source_batch_id"] == "BATCH_MON"]
check("周一重放产出 3 条重放事件记录", len(replay_mon) == 3)
check("重放事件接受状态为 2 已接受 1 重复跳过",
      sum(e["accept_status"] == replay.ACCEPT_OK for e in replay_mon) == 2
      and sum(e["accept_status"] == replay.ACCEPT_DUP for e in replay_mon) == 1)

# 7. 周一窗口补算结果：源记录数应为 5（3 实时 + 2 漏采补入），且尚未验明完整
corrections = replay.list_corrections(window_id=window_id)
mon_corr = [c for c in corrections if c["window_start"].startswith(MON_HOUR[:-3] + "00")]
check("周一窗口产生 1 个补算分组", len(mon_corr) == 1, str([(c["window_start"], c["status"]) for c in corrections]))
if mon_corr:
    mc = mon_corr[0]
    check("周一窗口源记录数为 5", mc["source_record_count"] == 5, f"实际 {mc['source_record_count']}")
    check("周一窗口 event_count 为 5", mc["event_count"] == 5, f"实际 {mc['event_count']}")
    check("未验明完整的窗口保持待验明", mc["status"] == replay.CORRECTION_UNVERIFIED)

# 日窗口同样应得到 5 条源记录
day_corr = [c for c in replay.list_corrections(window_id=window_b_id)
            if c["window_start"].startswith(MON_HOUR[:10] + "T00:00:00")]
check("日窗口周一桶源记录数同样为 5（跨窗口各自贡献一次）",
      len(day_corr) == 1 and day_corr[0]["source_record_count"] == 5,
      str([(c["window_start"], c["source_record_count"], c["status"]) for c in day_corr]))

# 8. 重复提交同组请求：不多计
resubmit = replay.submit_replay_group(["BATCH_TUE", "BATCH_MON"])  # 顺序调换也应识别
check("重复提交被幂等拦截", resubmit["ok"] and resubmit.get("idempotent") is True)
replay_event_count = db.count("replay_events", "replay_group_id=?", [gid])
check("重放事件记录未增加（仍为 3 条）", replay_event_count == 3, f"实际 {replay_event_count}")
mon_corr2 = next(c for c in replay.list_corrections(window_id=window_id)
                 if c["window_start"].startswith(MON_HOUR[:-3] + "00"))
check("重复提交后周一源记录数仍为 5", mon_corr2["source_record_count"] == 5)
ledger_count = db.count(
    "agg_correction_contributions",
    "window_id=? AND window_start LIKE ?", [window_id, MON_HOUR[:-3] + "00%"])
check("周一窗口台账恰为 5 条（同事件同窗口一次）", ledger_count == 5, f"实际 {ledger_count}")

# 9. 周二样本恢复后续办
for name, ts in tue_source:
    insert_event(name, ts, "BATCH_TUE")
resume = replay.resume_replay_group(gid)
check("续办成功", resume["ok"])
group2 = resume["group"]
check("续办后回放组已完成", group2["status"] == replay.GROUP_DONE, group2["status"])
tue_src2 = next(s for s in group2["sources"] if s["source_batch_id"] == "BATCH_TUE")
check("周二批次已读取且 2 条全部接受",
      tue_src2["status"] == replay.SOURCE_READ and tue_src2["event_accepted"] == 2)

corrections2 = replay.list_corrections(window_id=window_id)
mc2 = next(c for c in corrections2 if c["window_start"].startswith(MON_HOUR[:-3] + "00"))
tc2 = next(c for c in corrections2 if c["window_start"].startswith(TUE_HOUR[:-3] + "00"))
check("周一窗口补算转为已修正且源记录数仍为 5（已计贡献未再加一次）",
      mc2["status"] == replay.CORRECTION_DONE and mc2["source_record_count"] == 5,
      f"{mc2['status']}/{mc2['source_record_count']}")
check("周二窗口补算已修正且源记录数为 2",
      tc2["status"] == replay.CORRECTION_DONE and tc2["source_record_count"] == 2,
      f"{tc2['status']}/{tc2['source_record_count']}")

# 10. 溯源回查：同组不同来源都能从修正结果查回
trace = replay.list_correction_sources(replay_group_id=gid)
batches_in_trace = {row["source_batch_id"] for row in trace}
check("溯源链包含同组的周一与周二两个原始批次",
      {"BATCH_MON", "BATCH_TUE"} <= batches_in_trace, str(sorted(batches_in_trace)))
mon_trace = [r for r in trace if r["source_batch_id"] == "BATCH_MON"]
check("周一溯源行标注的贡献数为 2（仅漏采补入的部分）",
      all(r["event_count"] == 2 for r in mon_trace) and len(mon_trace) >= 1,
      str([(r["window_name"], r["event_count"]) for r in mon_trace]))

detail = replay.get_correction(mc2["id"])
src_batches = {s["source_batch_id"] for s in detail["correction_sources"]}
check("单条补算详情可回查原始批次", src_batches == {"BATCH_MON"}, str(src_batches))

records = replay.window_source_records(window_id, mc2["window_start"])
fps = [r["event_fingerprint"] for r in records]
check("周一窗口源记录台账 5 条且指纹无重复", len(records) == 5 and len(set(fps)) == 5)
origins = {r["origin_type"] for r in records}
check("台账同时包含 LIVE 与 REPLAY 两类来源", origins == {replay.ORIGIN_LIVE, replay.ORIGIN_REPLAY})

# 11. 再次续办/重复提交仍保持稳定
replay.resume_replay_group(gid)
stable = replay.submit_replay_group(["BATCH_MON", "BATCH_TUE"])
check("全部完成后重复提交仍幂等", stable.get("idempotent") is True)
final_ledger = db.count(
    "agg_correction_contributions",
    "window_id=? AND window_start LIKE ?", [window_id, MON_HOUR[:-3] + "00%"])
check("最终周一窗口台账仍为 5 条", final_ledger == 5, f"实际 {final_ledger}")

print()
if failures:
    print(f"共 {len(failures)} 项失败: {failures}")
    sys.exit(1)
print("全部场景校验通过")
