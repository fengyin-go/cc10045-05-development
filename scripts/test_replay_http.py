"""HTTP 层冒烟：准备场景数据后通过真实 API 驱动整组重放与补算回查。"""
from __future__ import annotations

import json
import os
import sys
import urllib.parse
import urllib.request

os.environ.setdefault("CC10045_DB_DIR", "/tmp/replay_http_db")
sys.path.insert(0, "/workspace/backend")
from app.database import db  # noqa: E402

BASE = "http://127.0.0.1:8020/api/pipeline"
MON = "2026-12-07T10:00:"
TUE = "2026-12-08T11:00:"


def call(method: str, path: str, payload: dict | None = None) -> dict:
    if "?" in path:
        head, query = path.split("?", 1)
        path = head + "?" + urllib.parse.urlencode(
            urllib.parse.parse_qsl(query))
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode())


def insert(name: str, ts: str, batch: str) -> None:
    db.insert("events", {
        "event_name": name, "event_attrs": '{"user_id":"u_h","amount":10}',
        "trigger_condition": "http场景", "source_module": "event_tracker",
        "occurred_at": ts, "batch_id": batch, "payload_size": 128,
    })


# 场景数据
for n, t in [("h1", f"{MON}05"), ("h2", f"{MON}20"), ("h3", f"{MON}35")]:
    insert(n, t, "LIVE_HMON")
for n, t in [("h1", f"{MON}05"), ("h4", f"{MON}40"), ("h5", f"{MON}50")]:
    insert(n, t, "HBATCH_MON")
for n, t in [("k1", f"{TUE}05"), ("k2", f"{TUE}25")]:
    insert(n, t, "HBATCH_TUE")
db.delete("events", "batch_id='HBATCH_TUE'")  # 周二样本暂时缺失

# 1. 批次清单
batches = call("GET", "/replay/batches")
ids = {b["batch_id"] for b in batches["items"]}
assert "HBATCH_MON" in ids and "LIVE_HMON" in ids, ids
mon_batch = next(b for b in batches["items"] if b["batch_id"] == "HBATCH_MON")
assert mon_batch["business_date"] == "2026-12-07", mon_batch
print("[PASS] 历史批次清单可查，业务日期:", mon_batch["business_date"])

# 2. 整组提交
sub = call("POST", "/replay/groups",
           {"source_batch_ids": ["HBATCH_MON", "HBATCH_TUE"], "group_name": "HTTP跨日组"})
assert sub["ok"] and not sub.get("idempotent")
gid = sub["group"]["id"]
statuses = {s["source_batch_id"]: s["status"] for s in sub["group"]["sources"]}
assert statuses == {"HBATCH_MON": "已读取", "HBATCH_TUE": "样本缺失"}, statuses
print("[PASS] 整组提交：周一已读取、周二样本缺失，组状态", sub["group"]["status"])

# 3. 待验明窗口不得算完成
pending = call("GET", "/aggregation/corrections?status=待验明")
done = call("GET", "/aggregation/corrections?status=已修正")
assert pending["total"] > 0 and all(i["status"] == "待验明" for i in pending["items"])
assert all(i["status"] != "待验明" for i in done["items"])
print(f"[PASS] 待验明 {pending['total']} 个补算分组，未被当作完成补算")

# 4. 幂等重复提交
again = call("POST", "/replay/groups",
             {"source_batch_ids": ["HBATCH_TUE", "HBATCH_MON"]})
assert again.get("idempotent") is True, again
print("[PASS] 同组重放请求重复提交被幂等拦截")

# 5. 恢复周二并续办
for n, t in [("k1", f"{TUE}05"), ("k2", f"{TUE}25")]:
    insert(n, t, "HBATCH_TUE")
res = call("POST", f"/replay/groups/{gid}/resume")
assert res["group"]["status"] == "已完成", res["group"]["status"]
print("[PASS] 周二恢复后续办，回放组已完成")

# 6. 补算结果与 5 条源记录
corrs = call("GET", "/aggregation/corrections")
# 找到日汇总窗口（86400）周一桶
day = [c for c in corrs["items"]
       if c["window_start"] == "2026-12-07T00:00:00"]
assert day and all(c["status"] == "已修正" for c in day), [(c["window_name"], c["status"]) for c in day]
print(f"[PASS] 周一补算全部已修正；日窗口源记录数 {day[0]['source_record_count']}")
assert day[0]["source_record_count"] == 5, day[0]

# 7. 溯源回查：同组不同来源
trace = call("GET", f"/aggregation/corrections/sources?replay_group_id={gid}")
batches_in = {r["source_batch_id"] for r in trace["items"]}
assert {"HBATCH_MON", "HBATCH_TUE"} <= batches_in, batches_in
by_batch = {b: [r for r in trace["items"] if r["source_batch_id"] == b] for b in batches_in}
assert all(r["event_count"] == 2 for r in by_batch["HBATCH_MON"])
print("[PASS] 从修正结果可查回同组的两个原始批次（跨日期来源）")

# 8. 单条补算详情
one = call("GET", f"/aggregation/corrections/{day[0]['id']}")
assert one["correction"]["correction_sources"][0]["source_batch_id"] == "HBATCH_MON"
print("[PASS] 单条补算详情含原始批次溯源链")

# 9. 窗口源记录台账
led = call("GET", f"/aggregation/windows/{day[0]['window_id']}/correction-sources?window_start=2026-12-07T00:00:00")
assert led["total"] == 5 and len({r["event_fingerprint"] for r in led["items"]}) == 5
print("[PASS] 窗口源记录台账 5 条且指纹唯一（同一事件同一窗口仅贡献一次）")

print("\nHTTP 冒烟全部通过")
