"""中国 2022-2023 法定节假日 / 调休上班日。天气等外部特征以同样方式通过 daily.EXTRA_FEATURES 接入。"""
from __future__ import annotations

import pandas as pd


def _rng(a, b):
    return {d.strftime("%Y-%m-%d") for d in pd.date_range(a, b)}


HOLIDAYS = set().union(
    _rng("2022-09-10", "2022-09-12"),  # 中秋
    _rng("2022-10-01", "2022-10-07"),  # 国庆
    _rng("2022-12-31", "2023-01-02"),  # 元旦
    _rng("2023-01-21", "2023-01-27"),  # 春节
    _rng("2023-04-05", "2023-04-05"), _rng("2023-04-29", "2023-05-03"),
)
WORKDAYS = {"2022-10-08", "2022-10-09", "2023-01-28", "2023-01-29", "2023-04-23", "2023-05-06"}  # 调休上班


def holiday_frame(ts: pd.Series) -> pd.DataFrame:
    """输入小时时间戳序列，返回 is_holiday / is_adjusted_workday / is_offday 三列（int8）。"""
    d = pd.to_datetime(ts).dt.strftime("%Y-%m-%d")
    hol = d.isin(HOLIDAYS).astype("int8")
    work = d.isin(WORKDAYS).astype("int8")
    wkend = (pd.to_datetime(ts).dt.day_of_week >= 5).astype("int8")
    off = ((hol == 1) | ((wkend == 1) & (work == 0))).astype("int8")
    return pd.DataFrame({"is_holiday": hol.values, "is_adjusted_workday": work.values, "is_offday": off.values},
                        index=ts.index)
