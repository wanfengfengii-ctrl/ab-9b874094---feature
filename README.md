# 海洋观测浮标采样 API

浮标观测数据的原子批量接入与**稳定快照分页**服务。分析人员分页导出期间，
即使持续接收到新数据（含更早时刻的观测），也不会出现漏项、重复或页序漂移。

- 运行时：Node.js 22，**零第三方依赖**（内置 `node:sqlite`、`fetch`、`node:test`）
- 存储：SQLite（WAL 模式，持久化到 `/data`）
- 容器：多阶段 Dockerfile（`runtime` / `verify`）+ Docker Compose（健康检查、一次性 verify）

## 快速开始

```bash
# 宿主机端口可用环境变量配置（默认 8080）
HOST_PORT=18080 docker compose up -d --build api
curl http://localhost:18080/health

# 一键验证：构建镜像 -> 清洁启动 -> 代码测试 -> API 冒烟（以退出码汇报）
./scripts/verify.sh
docker compose logs verify     # 查看 verify 明细
```

本地开发（无需 Docker）：

```bash
npm test                 # 单元 + HTTP 集成测试（41 项）
npm run smoke            # 本地拉起服务冒烟，含进程重启后续游标/导出令牌
PORT=8080 npm start
```

## API

### `POST /api/streams/{streamId}/samples`

原子接收 **1–100** 条观测。请求体：

```json
{
  "samples": [
    { "sampleId": "buoy-001", "ts": "2026-10-05T08:30:00Z", "value": 42 },
    { "sampleId": "buoy-002", "ts": "2026-10-05T08:30:00+08:00", "value": -7 }
  ]
}
```

- `sampleId`：流内唯一（非空字符串）；`ts`：严格 RFC3339（带时区，内部归一化为 UTC）；`value`：整数。
- 批内重复，或与已有编号冲突 → **整批回滚**，返回 `409`，不留下任何部分写入。
- 成功返回 `200`：`{ "accepted": n, "firstSeq": x, "lastSeq": y }`。

错误码：`duplicate_in_batch` / `conflict_existing`（409），`invalid_request`（400）。

### `GET /api/streams/{streamId}/samples`

按 **(时刻, sampleId) 稳定升序**遍历，支持：

| 参数 | 说明 |
| --- | --- |
| `from` / `to` | RFC3339 闭区间时间范围，可缺省 |
| `pageSize` | 每页条数，1–1000，默认 100 |
| `cursor` | 上一页响应中的不透明游标 |

```json
{
  "items": [ { "sampleId": "buoy-001", "ts": "2026-10-05T08:30:00Z", "value": 42 } ],
  "pageSize": 100,
  "snapshotSeq": 128,
  "nextCursor": "eyJ2Ijox….<HMAC>",
  "done": false
}
```

### 快照一致性保证

- 每个无游标请求（会话首页）把**当前接收序号固定为快照上界 `snapshotSeq`**；
  后续页只可见 `seq <= snapshotSeq` 的数据。
- 遍历期间到达的任何数据——哪怕时刻更早——都不会进入本次遍历：
  **最终恰好返回快照中每项一次**，顺序恒定。
- 每次响应都带回同一个 `snapshotSeq`；遍历结束时 `nextCursor` 为 `null` 且 `done: true`。
- 需要新数据时重新发起一次无游标请求，即固定新的快照上界。
- 分页采用 keyset（`(ts, sampleId)` 元组比较）而非 OFFSET，插入不会导致漂移。

### 并行导出：`POST /api/streams/{streamId}/exports`

大型快照可由 **2–8 个工作进程并行翻页**导出。创建导出时锁定一份稳定快照，
各分片互不重叠，合并后恰好覆盖该快照中时间范围内的全部观测；导出期间
（含服务重启）到达的新数据一律不进入本次导出。

请求体（均可缺省，仅 `workerCount` 必填）：

```json
{
  "from": "2026-01-01T00:00:00Z",
  "to": "2026-12-31T23:59:59Z",
  "workerCount": 4
}
```

成功返回 `201`：

```json
{
  "exportToken": "eyJ2IjoyLCJrIjoiZS….<HMAC>",
  "snapshotSeq": 128,
  "workerCount": 4
}
```

- `exportToken` 为不透明签名令牌，锁定流、时间范围、快照序号与 worker 数；
  `workerCount` 允许 2..8，服务返回实际采用的工作进程数。
- 随后各工作进程在**现有 GET samples 请求**上携带以下查询参数：

| 参数 | 说明 |
| --- | --- |
| `exportToken` | 创建导出返回的令牌（必填） |
| `workerIndex` | 零基分片编号，`0..workerCount-1`（必填） |
| `pageSize` | 每页条数，1–1000，默认 100 |
| `cursor` | 该分片上一页返回的续页游标（首页不传） |

```json
{
  "items": [ /* 与普通分页相同的观测项 */ ],
  "pageSize": 100,
  "snapshotSeq": 128,
  "workerIndex": 0,
  "workerCount": 4,
  "nextCursor": "….<HMAC>",
  "done": false
}
```

**分片语义**：每条观测按 `(ts, sampleId)` 哈希落入固定 8 个桶，
worker `k` 负责所有满足 `bucket % workerCount == k` 的桶。因此：

- 各分片**互不相交**：每个桶只属于一个 worker；
- 合并**恰好覆盖**创建导出时范围内的全部观测，无漏项、无重复；
- **空分片**首页即返回 `done: true`、`items: []`，工作进程直接结束；
- 合法令牌及其游标在**服务重启后**仍可让各 worker 完成原快照
  （配合固定的 `CURSOR_SECRET` 与持久化数据卷）。
- 未携带 `exportToken` 的请求完全保持原批量接入与快照分页行为，向后兼容。

**导出误用的可区分错误（均为 400，且不返回任何数据）**：

| 情形 | error.code |
| --- | --- |
| 令牌篡改/伪造/格式损坏 | `export_token_signature_invalid` / `invalid_export_token` |
| 令牌跨流使用 | `export_stream_mismatch` |
| `workerIndex` 越界（≥ workerCount） | `worker_index_out_of_range` |
| 携带令牌但缺少 `workerIndex` | `worker_index_required` |
| 把某分片的游标用于另一个分片 | `shard_cursor_mismatch` |
| 分片游标篡改 | `shard_cursor_signature_invalid` / `invalid_shard_cursor` |
| 分片游标跨流 | `shard_stream_mismatch` |
| 分片游标脱离令牌走普通分页 | `export_token_required` |

### 游标安全与生命周期

- 游标为 `base64url(JSON).HMAC-SHA256`，不透明、不可伪造；密钥由环境变量
  `CURSOR_SECRET` 配置（compose 固定注入，故**服务重启后旧游标仍可继续**）。
- 游标载荷绑定流 ID、快照序号、原始时间范围与页大小。非法使用返回明确的 4xx：

| 情形 | 状态码 | error.code |
| --- | --- | --- |
| 篡改/伪造/格式损坏 | 400 | `cursor_signature_invalid` / `invalid_cursor` |
| 跨流复用 | 400 | `cursor_stream_mismatch` |
| 携带游标但改变原 `from`/`to` | 400 | `cursor_range_mismatch` |

### `GET /health`

容器健康检查端点，返回服务状态、当前接收序号与样本总数。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `HOST_PORT` | `8080` | 宿主机映射端口（compose 层） |
| `PORT` | `8080` | 容器内监听端口 |
| `HOST` | `0.0.0.0` | 监听地址 |
| `DB_PATH` | `/data/observations.db` | SQLite 数据文件 |
| `CURSOR_SECRET` | 随机（仅开发） | 游标 HMAC 密钥；生产必须固定以便重启续用 |

## verify 一次性服务

`docker compose up verify`（或 `./scripts/verify.sh`）会：

1. 等待 `api` 健康检查通过（`depends_on: service_healthy`）；
2. 在**清洁启动**的 API 上运行代码测试；
3. 插入初始数据、分页取首页，**在分页中途追加更早时刻的观测**，继续翻页并断言
   不漏项 / 不重复 / 页序不漂移、快照序号不变；
4. **并行导出冒烟**：多 worker 并发翻页断言分片不重叠且合并恰好覆盖快照、
   期间新增数据不进入、空分片立即结束，并覆盖令牌篡改 / 跨流 /
   workerIndex 越界 / 游标用于另一分片等错误路径；
5. 覆盖批内重复、编号冲突整批拒绝、游标篡改、跨流复用、改变时间范围等错误路径。

全部通过退出码为 `0`，任一失败非零。
