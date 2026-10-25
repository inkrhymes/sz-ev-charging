-- 每日预测（forecast/daily.py 写入）与每日误差（forecast/evaluate.py 写入）。按天先删后插，幂等。
CREATE TABLE IF NOT EXISTS ads_forecast_zone_hour (
  taz_id        VARCHAR(32) NOT NULL,
  ts_hour       DATETIME    NOT NULL,
  y_hat_lgbm    DOUBLE      NULL,
  y_hat_lag1    DOUBLE      NULL COMMENT '日前朴素基线：预测时最后一个已知小时值，24 小时都用它',
  model_version VARCHAR(64) NOT NULL,
  trained_until DATETIME    NOT NULL,
  created_at    DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (taz_id, ts_hour),
  KEY idx_ts (ts_hour)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ads_forecast_metric_daily (
  ds            DATE        NOT NULL PRIMARY KEY,
  n             INT         NOT NULL,
  mae_lgbm      DOUBLE      NULL,
  rmse_lgbm     DOUBLE      NULL,
  mae_lag1      DOUBLE      NULL,
  rmse_lag1     DOUBLE      NULL,
  mae_lag24     DOUBLE      NULL,
  model_version VARCHAR(64) NULL,
  drift         TINYINT     NOT NULL DEFAULT 0,
  created_at    DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
