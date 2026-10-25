#!/usr/bin/env bash
# 停止服务；加 --purge 同时删除数据卷
cd "$(dirname "$0")/.."
if [ "${1:-}" = "--purge" ]; then docker compose down -v; else docker compose down; fi
