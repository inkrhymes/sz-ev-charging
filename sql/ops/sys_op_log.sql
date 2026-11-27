-- 系统操作审计日志（模块 6：登录、退出、告警处理、工单变更、数据导出、权限拦截）
CREATE TABLE IF NOT EXISTS sys_op_log (
  log_id      BIGINT AUTO_INCREMENT PRIMARY KEY,
  actor       VARCHAR(64)  NOT NULL COMMENT '操作人账号',
  role        VARCHAR(16)  NOT NULL COMMENT '角色 admin / readonly',
  action      VARCHAR(32)  NOT NULL COMMENT 'login / logout / alert_update / ticket_create / ticket_update / export / auth_denied',
  target_type VARCHAR(32)  NULL     COMMENT 'auth / alert / ticket / export',
  target_id   VARCHAR(64)  NULL     COMMENT '关联对象编号',
  detail      VARCHAR(500) NULL     COMMENT '操作说明',
  result      VARCHAR(16)  NOT NULL DEFAULT 'ok' COMMENT 'ok / denied / failed',
  ip          VARCHAR(64)  NULL     COMMENT '来源 IP',
  created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_created (created_at),
  KEY idx_action (action),
  KEY idx_actor (actor)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
