# syntax=docker/dockerfile:1

# ---- 基础阶段：零运行时依赖（仅使用 Node 22 内置能力：node:sqlite / fetch / node:test）----
FROM node:22-slim AS base
WORKDIR /app
ENV NODE_NO_WARNINGS=1 \
    PORT=8080 \
    HOST=0.0.0.0 \
    DB_PATH=/data/observations.db
COPY package.json ./
COPY src ./src
COPY test ./test
EXPOSE 8080

# ---- 运行阶段：API 服务 ----
FROM base AS runtime
# 预建数据目录并交给 node 用户：命名卷首次挂载会继承该属主，
# 否则非 root 运行时 SQLite 无法在卷内建库。
RUN mkdir -p /data && chown node:node /data
USER node
CMD ["node", "src/server.js"]

# ---- 验证阶段：一次性 verify 服务 ----
# 先跑代码测试，再对已就绪的 API 做冒烟（BASE_URL 由 compose 注入）。
# 任一环节失败，shell 以非零退出码汇报。
FROM base AS verify
CMD ["sh", "-c", "node --test test/*.test.mjs && node test/smoke.mjs"]
