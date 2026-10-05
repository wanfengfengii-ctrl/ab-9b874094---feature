#!/bin/sh
# 一键验证：镜像构建 -> 代码测试 -> API 冒烟（含分页中途插入早时刻数据）。
# 任一步失败，脚本即以该步退出码结束（可直接用于 CI）。
set -eu

cd "$(dirname "$0")/.."

cleanup() {
  echo "==> 清理"
  docker compose down --remove-orphans || true
}
trap cleanup EXIT

echo "==> [1/2] 构建镜像（api + verify）"
docker compose build

echo "==> [2/2] 清洁启动并运行 verify（代码测试 + API 冒烟）"
# 只启动 verify 及其依赖 api；--exit-code-from 透传一次性 verify 的退出码
docker compose up \
  --build \
  --abort-on-container-exit \
  --exit-code-from verify \
  verify
