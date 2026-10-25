#!/usr/bin/env python3
"""按当前湖里的 ods/dwd/ads 分区重跑质检，重建 dq_alert 里的质检规则告警。

- 只删除/重建质检规则行，rule_name='forecast_drift'（预测漂移，forecast 写入）不动。
- ads_ticket 保留：按 (rule_name, ds, station_id) 找到重建后的新告警重新关联，旧告警的处理状态/备注带过去。
  python orchestration/rebuild_dq_alerts.py [--start 2022-09-01 --end 2023-02-28]
需要 SZ_EV_LAKE 指向真实数据湖。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from orchestration import tasks  # noqa: E402
from quality import engine  # noqa: E402

KEEP = ("forecast_drift",)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2022-09-01")
    ap.add_argument("--end", default="2023-02-28")
    a = ap.parse_args(argv)
    con = engine._connect()
    with con.cursor() as cur:
        cur.execute("SELECT t.ticket_id, t.alert_id, a.rule_name, DATE_FORMAT(a.ds,'%%Y-%%m-%%d') ds, a.layer, a.station_id,"
                    " a.status, a.note FROM ads_ticket t LEFT JOIN dq_alert a ON a.alert_id=t.alert_id", ())
        links = cur.fetchall()
        n = cur.execute("DELETE FROM dq_alert WHERE rule_name NOT IN %s", (KEEP,))
        print(f"[rebuild] 删除质检告警 {n} 行（保留 {KEEP}）")
    blocked = []
    for ds in pd.date_range(a.start, a.end).strftime("%Y-%m-%d"):
        for layer in ("ods", "dwd", "ads"):
            try:
                tasks.dq(layer, ds, write_db=True)
            except engine.DQBlockError as e:
                blocked.append(f"{ds} {layer}: {e}")
    print(f"[rebuild] block {len(blocked)} 天层：", *blocked, sep="\n  ")
    with con.cursor() as cur:
        for t in links:
            if not t[2]:
                continue
            tid, _, rule, ds, layer, sid, st, note = t
            q = ("SELECT alert_id FROM dq_alert WHERE rule_name=%s AND layer=%s AND station_id <=> %s "
                 "ORDER BY ds=%s DESC, ABS(DATEDIFF(ds,%s)), alert_id LIMIT 1")
            cur.execute(q, (rule, layer, sid, ds, ds))
            r = cur.fetchone()
            if not r and sid:
                cur.execute("SELECT alert_id FROM dq_alert WHERE station_id=%s ORDER BY ABS(DATEDIFF(ds,%s)) LIMIT 1", (sid, ds))
                r = cur.fetchone()
            new = r[0] if r else None
            cur.execute("UPDATE ads_ticket SET alert_id=%s, updated_at=updated_at WHERE ticket_id=%s", (new, tid))
            if new and st != "open":
                cur.execute("UPDATE dq_alert SET status=%s, note=%s WHERE alert_id=%s", (st, note, new))
            print(f"[rebuild] 工单 {tid}: 告警 {t[1]} ({rule} {ds} 站 {sid}) -> {new}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
