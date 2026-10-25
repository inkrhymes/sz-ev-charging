"""sz_ev_daily：按天分区 ds 的离线链路，质检不通过即阻断下游。

ods_ingest -> dq_ods -> dwd_build -> dq_dwd -> ads_build -> dq_ads -> publish
publish -> forecast_daily -> forecast_evaluate；publish -> weather_join；publish -> site_suggest
扩展任务退出码：0 成功，2 跳过（AirflowSkipException），3 重试（retries=2），其他直接失败不重试。
任务本身只调用 orchestration/tasks.py 里的薄包装，具体逻辑在各负责人的脚本中。
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

REPO = os.getenv("REPO_ROOT", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.insert(0, REPO)

from airflow import DAG  # noqa: E402
from airflow.exceptions import AirflowFailException, AirflowSkipException  # noqa: E402
from airflow.operators.python import PythonOperator  # noqa: E402

from orchestration import tasks  # noqa: E402

log = logging.getLogger(__name__)


def on_failure(context):
    ti = context["task_instance"]
    log.error("任务失败 dag=%s task=%s ds=%s", ti.dag_id, ti.task_id, context["ds"])
    # 邮件告警钩子（默认关闭）：配置 SMTP 后把 ALERT_EMAIL 设为收件人即可
    if os.getenv("ALERT_EMAIL"):
        from airflow.utils.email import send_email
        send_email(os.environ["ALERT_EMAIL"], f"[sz_ev] {ti.task_id} {context['ds']} 失败", str(context.get("exception")))


default_args = {
    "owner": "xiaobai",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure,
}

with DAG(
    dag_id="sz_ev_daily",
    start_date=datetime(2022, 9, 1),
    end_date=datetime(2023, 2, 28),
    schedule="@daily",
    catchup=False,           # 历史数据用 backfill 显式补跑
    max_active_runs=1,       # 单机内存有限，一次只跑一天
    default_args=default_args,
    tags=["sz_ev", "daily"],
) as dag:
    def build(name, fn):
        return PythonOperator(task_id=name, python_callable=tasks.recorded(name, fn, "airflow"),
                              op_kwargs={"ds": "{{ ds }}"})

    def gate(layer):
        # 质检失败直接抛 DQBlockError，不需要重试
        return PythonOperator(task_id=f"dq_{layer}", python_callable=tasks.recorded(f"dq_{layer}", dict(tasks.CHAIN)[f"dq_{layer}"], "airflow"),
                              op_kwargs={"ds": "{{ ds }}"}, retries=0)

    chain = [build("ods_ingest", tasks.ods_ingest), gate("ods"),
             build("dwd_build", tasks.dwd_build), gate("dwd"),
             build("ads_build", tasks.ads_build), gate("ads"),
             build("publish", tasks.publish)]
    for a, b in zip(chain, chain[1:]):
        a >> b

    def extra(name, fn, **kw):
        inner = tasks.recorded(name, fn, "airflow")

        def call(ds):
            try:
                return inner(ds)
            except tasks.TaskSkipped as e:
                raise AirflowSkipException(str(e)) from e
            except tasks.TaskRetryable:
                raise                                  # 走 retries=2
            except Exception as e:
                raise AirflowFailException(str(e)) from e
        call.__name__ = name
        return PythonOperator(task_id=name, python_callable=call, op_kwargs={"ds": "{{ ds }}"}, retries=2, **kw)

    ex = dict(tasks.AFTER_PUBLISH)
    publish = chain[-1]
    f_daily = extra("forecast_daily", ex["forecast_daily"])
    # 评估的是 ds 当天（前一天的预测）：今天的预测被跳过也照样评估；预测失败或 publish 没成功则不评估
    f_eval = extra("forecast_evaluate", ex["forecast_evaluate"], trigger_rule="none_failed")
    publish >> f_daily >> f_eval
    publish >> f_eval
    publish >> extra("weather_join", ex["weather_join"])
    publish >> extra("site_suggest", ex["site_suggest"])
