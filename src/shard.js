/**
 * 并行导出分片拓扑。
 *
 * 样本在写入时按 (ts, sampleId) 哈希落入固定的 {@link MAX_BUCKETS} 个桶；
 * 一次导出的 worker 数 w（2..8）中，workerIndex 为 k 的工作进程负责
 * 所有满足 b % w === k 的桶。该划分：
 *  - 互不相交（每个桶只属于一个 worker）；
 *  - 恰好覆盖（0..MAX_BUCKETS-1 全部分配出去）；
 *  - 与数据内容无关，创建导出时无需串行预扫描或记录边界。
 */

import { createHash } from 'node:crypto';

export const MIN_WORKERS = 2;
export const MAX_WORKERS = 8;
export const MAX_BUCKETS = 8;

/**
 * 样本归属桶：对分片键 (ts, sampleId) 做 SHA-256 后取模。
 * 同时刻数据由 sampleId 打散，天然均匀，且只依赖键本身（确定性，重启不变）。
 */
export function bucketOf(ts, sampleId) {
  const h = createHash('sha256').update(ts).update('').update(sampleId).digest();
  // 大端读前 4 字节取模
  return h.readUInt32BE(0) % MAX_BUCKETS;
}

/** worker k 在 workerCount=w 时负责的桶（升序）。 */
export function shardBuckets(workerCount, workerIndex) {
  const out = [];
  for (let b = workerIndex; b < MAX_BUCKETS; b += workerCount) {
    out.push(b);
  }
  return out;
}

/** 断言 workerIndex 合法，返回其负责的桶列表。 */
export function bucketsForWorker(workerCount, workerIndex) {
  if (!Number.isInteger(workerCount) ||
      workerCount < MIN_WORKERS || workerCount > MAX_WORKERS) {
    throw new Error(`workerCount 允许范围为 ${MIN_WORKERS}..${MAX_WORKERS}`);
  }
  if (!Number.isInteger(workerIndex) ||
      workerIndex < 0 || workerIndex >= workerCount) {
    throw new Error(`workerIndex 必须为 0..${workerCount - 1} 的整数`);
  }
  return shardBuckets(workerCount, workerIndex);
}
