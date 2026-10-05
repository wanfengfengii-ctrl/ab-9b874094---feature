import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { Store, BatchConflictError } from '../src/db.js';
import {
  issueCursor, parseCursor, bindCursor, CursorError,
  issueExportToken, parseExportToken, bindExportToken,
  issueShardCursor, parseShardCursor, bindShardCursor,
} from '../src/cursor.js';
import {
  validateBatch, validateQuery, validateExport, ValidationError,
} from '../src/validation.js';
import {
  bucketOf, shardBuckets, bucketsForWorker,
  MIN_WORKERS, MAX_WORKERS, MAX_BUCKETS,
} from '../src/shard.js';
import { DatabaseSync } from 'node:sqlite';

const R = (ts, sid, v = 1) => ({ sampleId: sid, ts, value: v });

test('Store: 批次写入分配连续接收序号', () => {
  const s = new Store(':memory:');
  assert.equal(s.currentSeq(), 0);
  const r1 = s.insertBatch('st', [R('2026-01-01T00:00:00Z', 'a'), R('2026-01-01T00:00:01Z', 'b')]);
  assert.deepEqual([r1.firstSeq, r1.lastSeq, r1.count], [1, 2, 2]);
  const r2 = s.insertBatch('st', [R('2026-01-01T00:00:02Z', 'c')]);
  assert.deepEqual([r2.firstSeq, r2.lastSeq], [3, 3]);
  assert.equal(s.currentSeq(), 3);
  s.close();
});

test('Store: 批内重复 -> 整批拒绝且无部分写入', () => {
  const s = new Store(':memory:');
  s.insertBatch('st', [R('2026-01-01T00:00:00Z', 'a')]);
  assert.throws(
    () => s.insertBatch('st', [
      R('2026-01-01T00:00:05Z', 'x'),
      R('2026-01-01T00:00:06Z', 'x'),
    ]),
    (e) => e instanceof BatchConflictError && e.details.reason === 'duplicate_in_batch'
  );
  assert.equal(s.totalSamples(), 1);
  assert.equal(s.currentSeq(), 1, '冲突不得占用序号');
  s.close();
});

test('Store: 与已有编号冲突 -> 整批回滚', () => {
  const s = new Store(':memory:');
  s.insertBatch('st', [
    R('2026-01-01T00:00:00Z', 'a'),
    R('2026-01-01T00:00:01Z', 'b'),
  ]);
  assert.throws(
    () => s.insertBatch('st', [
      R('2026-01-02T00:00:00Z', 'c'),
      R('2026-01-02T00:00:01Z', 'a'), // 与已有冲突
      R('2026-01-02T00:00:02Z', 'd'),
    ]),
    (e) => e instanceof BatchConflictError && e.details.reason === 'conflict_existing'
  );
  assert.equal(s.totalSamples(), 2);
  assert.equal(s.currentSeq(), 2);
  // c/d 都不得留下
  assert.equal(s.readPage('st', s.currentSeq(), { fromTs: '0', toTs: 'z' }, null, 100).length, 2);
  s.close();
});

test('Store: 同一样本能在不同流中各自存在', () => {
  const s = new Store(':memory:');
  s.insertBatch('s1', [R('2026-01-01T00:00:00Z', 'a')]);
  s.insertBatch('s2', [R('2026-01-01T00:00:00Z', 'a')]); // 不应冲突
  assert.equal(s.totalSamples(), 2);
  s.close();
});

test('Store: 快照按 (ts, sampleId) 稳定排序且快照上界隔离新写入', () => {
  const s = new Store(':memory:');
  s.insertBatch('st', [
    R('2026-01-01T00:00:02Z', 'c'),
    R('2026-01-01T00:00:01Z', 'b'),
    R('2026-01-01T00:00:01Z', 'a'), // 与 b 同时刻 -> sampleId 次序
  ]);
  const snap = s.currentSeq(); // 3

  // 快照固定后插入“更早时刻”的观测
  s.insertBatch('st', [
    R('2025-12-31T23:59:59Z', 'early'),
    R('2026-01-01T00:00:03Z', 'late'),
  ]);

  const all = [];
  let after = null;
  for (;;) {
    const page = s.readPage('st', snap, { fromTs: '0', toTs: 'z' }, after, 2);
    all.push(...page);
    if (page.length < 2) break;
    const last = page[page.length - 1];
    after = { ts: last.ts, sampleId: last.sampleId };
  }
  assert.deepEqual(all.map((x) => x.sampleId), ['a', 'b', 'c']);
  assert.equal(all.length, 3, '快照外的两条（更早/更晚）均不得出现');

  // 新快照包含全部 5 条，且 early 排在最前
  const fresh = s.readPage('st', s.currentSeq(), { fromTs: '0', toTs: 'z' }, null, 100);
  assert.deepEqual(fresh.map((x) => x.sampleId), ['early', 'a', 'b', 'c', 'late']);
  s.close();
});

test('Store: 时间范围过滤（闭区间）', () => {
  const s = new Store(':memory:');
  s.insertBatch('st', [
    R('2026-01-01T00:00:00Z', 'a'),
    R('2026-01-01T00:00:05Z', 'b'),
    R('2026-01-01T00:00:10Z', 'c'),
  ]);
  const seq = s.currentSeq();
  const rows = s.readPage(
    'st', seq,
    { fromTs: '2026-01-01T00:00:05Z', toTs: '2026-01-01T00:00:10Z' },
    null, 100
  );
  assert.deepEqual(rows.map((r) => r.sampleId), ['b', 'c']);
  s.close();
});

test('游标: 签发/解析往返', () => {
  const secret = 'test-secret';
  const payload = {
    v: 1, s: 'st1', q: 42,
    f: '2026-01-01T00:00:00Z', t: '9999-12-31T23:59:59.999999999Z',
    p: 10, a: { ts: '2026-01-01T00:00:01Z', i: 'x' },
  };
  const c = issueCursor(secret, payload);
  const got = parseCursor(secret, c);
  assert.deepEqual(got, payload);
  assert.equal(typeof c, 'string');
});

test('游标: 篡改载荷或签名 -> 明确错误', () => {
  const c = issueCursor('sec', {
    v: 1, s: 'st', q: 1, f: 'a', t: 'b', p: 10, a: null,
  });
  const [body, sig] = c.split('.');
  assert.throws(() => parseCursor('sec', body + '.' + sig.replace(/./, 'X')),
    (e) => e instanceof CursorError && e.code === 'cursor_signature_invalid');
  // 用另一个密钥也无法通过
  assert.throws(() => parseCursor('other', c),
    (e) => e instanceof CursorError && e.code === 'cursor_signature_invalid');
  assert.throws(() => parseCursor('sec', 'garbage'),
    (e) => e instanceof CursorError && e.code === 'invalid_cursor');
  assert.throws(() => parseCursor('sec', ''),
    (e) => e instanceof CursorError && e.code === 'invalid_cursor');
});

test('游标: 跨流复用 / 改变时间范围 -> 明确的客户端错误', () => {
  const p = {
    v: 1, s: 'stream-A', q: 9,
    f: '2026-01-01T00:00:00Z', t: '9999-12-31T23:59:59.999999999Z',
    p: 25, a: null,
  };
  assert.throws(
    () => bindCursor(p, { streamId: 'stream-B', fromTs: null, toTs: null, pageSize: null }),
    (e) => e instanceof CursorError && e.code === 'cursor_stream_mismatch'
  );
  assert.throws(
    () => bindCursor(p, {
      streamId: 'stream-A',
      fromTs: '2025-01-01T00:00:00Z', toTs: null, pageSize: null,
    }),
    (e) => e instanceof CursorError && e.code === 'cursor_range_mismatch'
  );
  assert.throws(
    () => bindCursor(p, {
      streamId: 'stream-A',
      fromTs: null, to: null, toTs: '2027-01-01T00:00:00Z', pageSize: null,
    }),
    (e) => e instanceof CursorError && e.code === 'cursor_range_mismatch'
  );
  // 合法绑定
  const bound = bindCursor(p, { streamId: 'stream-A', fromTs: null, toTs: null, pageSize: null });
  assert.equal(bound.snapshotSeq, 9);
});

test('校验: 批次大小 1..100', () => {
  assert.throws(() => validateBatch({ samples: [] }), ValidationError);
  assert.throws(() => validateBatch({ samples: 'no' }), ValidationError);
  const ok = validateBatch({ samples: Array.from({ length: 100 }, (_, i) =>
    R('2026-01-01T00:00:00Z', `s${i}`)) });
  assert.equal(ok.items.length, 100);
  assert.throws(() => validateBatch({ samples: Array.from({ length: 101 }, (_, i) =>
    R('2026-01-01T00:00:00Z', `s${i}`)) }), ValidationError);
});

test('校验: sampleId / ts / value 规则', () => {
  assert.throws(() => validateBatch({ samples: [{ ts: '2026-01-01T00:00:00Z', value: 1 }] }),
    ValidationError);
  assert.throws(() => validateBatch({ samples: [R('not-a-time', 'a')] }), ValidationError);
  assert.throws(() => validateBatch({ samples: [R('2026-01-01 00:00:00', 'a')] }),
    ValidationError, '缺时区偏移应拒绝');
  assert.throws(() => validateBatch({ samples: [
    { sampleId: 'a', ts: '2026-01-01T00:00:00Z', value: 1.5 }] }), ValidationError);
  assert.throws(() => validateBatch({ samples: [
    { sampleId: 'a', ts: '2026-01-01T00:00:00Z', value: '1' }] }), ValidationError);
});

test('校验: 带偏移时间归一化为 UTC 字符串（字典序可比较）', () => {
  const { items } = validateBatch({ samples: [
    { sampleId: 'a', ts: '2026-01-01T08:00:00+08:00', value: -5 },
  ] });
  assert.equal(items[0].ts, '2026-01-01T00:00:00.000000000Z');
  assert.equal(items[0].value, -5);
});

test('校验: 小数秒补齐 9 位，字典序与时间序一致', () => {
  const { items } = validateBatch({ samples: [
    { sampleId: 'a', ts: '2026-01-01T00:00:00.1Z', value: 1 },
    { sampleId: 'b', ts: '2026-01-01T00:00:00.09Z', value: 2 },
    { sampleId: 'c', ts: '2026-01-01T00:00:00.19Z', value: 3 },
  ] });
  const ts = Object.fromEntries(items.map((i) => [i.sampleId, i.ts]));
  assert.equal(ts.a, '2026-01-01T00:00:00.100000000Z');
  assert.equal(ts.b, '2026-01-01T00:00:00.090000000Z');
  assert.equal(ts.c, '2026-01-01T00:00:00.190000000Z');
  assert.ok(ts.b < ts.a && ts.a < ts.c, '0.09 < 0.1 < 0.19');
});

test('校验: 查询参数 pageSize / 时间范围', () => {
  const q = validateQuery(new URLSearchParams('pageSize=50&from=2026-01-01T00:00:00Z'));
  assert.equal(q.pageSize, 50);
  assert.equal(q.fromTs, '2026-01-01T00:00:00.000000000Z');
  assert.throws(() => validateQuery(new URLSearchParams('pageSize=0')), ValidationError);
  assert.throws(() => validateQuery(new URLSearchParams('pageSize=abc')), ValidationError);
  assert.throws(
    () => validateQuery(new URLSearchParams(
      'from=2026-02-01T00:00:00Z&to=2026-01-01T00:00:00Z')),
    ValidationError
  );
});

/* ============================ 并行导出：分片拓扑 ============================ */

test('分片: 桶划分互不相交且恰好覆盖 0..7', () => {
  assert.equal(MAX_BUCKETS, 8);
  for (let w = MIN_WORKERS; w <= MAX_WORKERS; w++) {
    const all = [];
    for (let k = 0; k < w; k++) {
      const bs = shardBuckets(w, k);
      // 每个桶都满足 b % w === k
      assert.ok(bs.every((b) => b % w === k), `w=${w} k=${k}`);
      all.push(...bs);
    }
    assert.deepEqual([...all].sort((a, b) => a - b), [0, 1, 2, 3, 4, 5, 6, 7],
      `w=${w}：并集恰好为全部桶`);
    assert.equal(new Set(all).size, 8, `w=${w}：桶互不重叠`);
  }
});

test('分片: workerIndex 越界被拒绝', () => {
  assert.throws(() => bucketsForWorker(2, -1));
  assert.throws(() => bucketsForWorker(2, 2));
  assert.throws(() => bucketsForWorker(8, 8));
  assert.throws(() => bucketsForWorker(1, 0));
  assert.throws(() => bucketsForWorker(9, 0));
  assert.deepEqual(bucketsForWorker(8, 7), [7]);
  assert.deepEqual(bucketsForWorker(2, 0), [0, 2, 4, 6]);
  assert.deepEqual(bucketsForWorker(2, 1), [1, 3, 5, 7]);
});

test('分片: bucketOf 确定且在范围内，同时刻按 sampleId 打散', () => {
  const b1 = bucketOf('2026-01-01T00:00:00Z', 'x');
  assert.equal(b1, bucketOf('2026-01-01T00:00:00Z', 'x'), '确定性');
  assert.ok(Number.isInteger(b1) && b1 >= 0 && b1 < MAX_BUCKETS);
  // 100 个不同 sampleId 不应全部挤在一个桶
  const buckets = new Set(
    Array.from({ length: 100 }, (_, i) => bucketOf('2026-01-01T00:00:00Z', `id-${i}`))
  );
  assert.ok(buckets.size > 1);
});

/* ============================ 并行导出：存储层 ============================ */

test('Store: 分片读取不重不漏且与全量快照一致', () => {
  const s = new Store(':memory:');
  const items = Array.from({ length: 60 }, (_, i) => ({
    sampleId: `m${String(i).padStart(2, '0')}`,
    ts: `2026-01-01T00:00:${String(i % 17).padStart(2, '0')}Z`,
    value: i,
  }));
  // 分多批写入
  s.insertBatch('st', items.slice(0, 30));
  s.insertBatch('st', items.slice(30));
  const snap = s.currentSeq();
  const RANGE = { fromTs: '0', toTs: 'z' };

  for (const w of [2, 3, 5, 8]) {
    const collected = [];
    for (let k = 0; k < w; k++) {
      const buckets = bucketsForWorker(w, k);
      let after = null;
      for (;;) {
        const page = s.readShardPage('st', snap, RANGE, buckets, after, 7);
        collected.push(...page);
        if (page.length < 7) break;
        const last = page[page.length - 1];
        after = { bucket: last.bucket, ts: last.ts, sampleId: last.sampleId };
      }
    }
    assert.equal(collected.length, 60, `w=${w} 总数`);
    assert.equal(new Set(collected.map((r) => r.sampleId)).size, 60,
      `w=${w} 无重复`);
    // 每条都落在其所属 worker 的桶里
    assert.ok(collected.every((r) =>
      r.bucket === bucketOf(r.ts, r.sampleId)), `w=${w} 桶归属正确`);
    // 与不分片的全量快照内容一致
    const want = s.readPage('st', snap, RANGE, null, 1000)
      .map((r) => r.sampleId).sort();
    assert.deepEqual(collected.map((r) => r.sampleId).sort(), want,
      `w=${w} 合并后等于全量快照`);
  }
  s.close();
});

test('Store: 分片读取遵循快照上界与时间范围', () => {
  const s = new Store(':memory:');
  s.insertBatch('st', [
    R('2026-01-01T00:00:00Z', 'a'),
    R('2026-01-02T00:00:00Z', 'b'),
  ]);
  const snap = s.currentSeq();
  s.insertBatch('st', [
    R('2025-12-31T00:00:00Z', 'early'),
    R('2026-01-03T00:00:00Z', 'late'),
  ]);
  for (let w = 2; w <= 8; w++) {
    let n = 0;
    for (let k = 0; k < w; k++) {
      n += s.readShardPage('st', snap,
        { fromTs: '0', toTs: 'z' }, bucketsForWorker(w, k), null, 100).length;
    }
    assert.equal(n, 2, `w=${w}：快照外数据不进入分片`);
  }
  // 时间范围
  let n = 0;
  for (let k = 0; k < 2; k++) {
    n += s.readShardPage('st', snap,
      { fromTs: '2026-01-01T00:00:00Z', toTs: '2026-01-01T00:00:00Z' },
      bucketsForWorker(2, k), null, 100).length;
  }
  assert.equal(n, 1, '仅 a 在范围内');
  s.close();
});

test('Store: 旧库（无 bucket 列）启动时迁移并回填', () => {
  const dir2 = mkdtempSync(join(tmpdir(), 'obs-migrate-'));
  const path = join(dir2, 'old.db');
  // 用旧结构手工建库
  const legacy = new DatabaseSync(path);
  legacy.exec('PRAGMA journal_mode=WAL');
  legacy.exec(`
    CREATE TABLE samples (
      stream_id TEXT NOT NULL, sample_id TEXT NOT NULL,
      ts TEXT NOT NULL, value INTEGER NOT NULL, seq INTEGER NOT NULL,
      PRIMARY KEY (stream_id, sample_id)
    ) WITHOUT ROWID
  `);
  legacy.exec('CREATE TABLE seq_meta (id INTEGER PRIMARY KEY CHECK (id=1), last_seq INTEGER NOT NULL)');
  legacy.exec('INSERT INTO seq_meta VALUES (1, 2)');
  const li = legacy.prepare(
    'INSERT INTO samples (stream_id, sample_id, ts, value, seq) VALUES (?,?,?,?,?)');
  li.run('st', 'a', '2026-01-01T00:00:00.000000000Z', 1, 1);
  li.run('st', 'b', '2026-01-02T00:00:00.000000000Z', 2, 2);
  legacy.close();

  // 以新版 Store 打开 -> 触发迁移
  const s = new Store(path);
  assert.equal(s.currentSeq(), 2);
  const rows = s.readPage('st', s.currentSeq(),
    { fromTs: '0', toTs: 'z' }, null, 100);
  assert.deepEqual(rows.map((r) => r.sampleId), ['a', 'b']);
  // 桶已回填，且可用于分片
  let n = 0;
  for (let k = 0; k < 2; k++) {
    n += s.readShardPage('st', s.currentSeq(),
      { fromTs: '0', toTs: 'z' }, bucketsForWorker(2, k), null, 100).length;
  }
  assert.equal(n, 2, '迁移后分片合计仍为 2 条');
  // 迁移幂等：再次打开不报错
  s.close();
  const s2 = new Store(path);
  assert.equal(s2.totalSamples(), 2);
  s2.close();
  rmSync(dir2, { recursive: true, force: true });
});

/* ============================ 并行导出：令牌与分片游标 ============================ */

const EXP = {
  id: 'exp1', streamId: 'st1', snapshotSeq: 42, workerCount: 4,
  fromTs: '2026-01-01T00:00:00.000000000Z',
  toTs: '9999-12-31T23:59:59.999999999Z',
};

test('导出令牌: 签发/解析往返', () => {
  const t = issueExportToken('sec', EXP);
  assert.equal(typeof t, 'string');
  assert.deepEqual(parseExportToken('sec', t), EXP);
});

test('导出令牌: 篡改/错误密钥/垃圾 -> 明确错误', () => {
  const t = issueExportToken('sec', EXP);
  const mid = Math.floor(t.length / 2);
  const flipped = t.slice(0, mid) + (t[mid] === 'A' ? 'B' : 'A') + t.slice(mid + 1);
  assert.throws(() => parseExportToken('sec', flipped),
    (e) => e instanceof CursorError && e.code === 'export_token_signature_invalid');
  assert.throws(() => parseExportToken('other', t),
    (e) => e instanceof CursorError && e.code === 'export_token_signature_invalid');
  assert.throws(() => parseExportToken('sec', 'garbage'),
    (e) => e instanceof CursorError && e.code === 'invalid_export_token');
});

test('导出令牌: 跨流绑定被拒绝', () => {
  const t = parseExportToken('sec', issueExportToken('sec', EXP));
  assert.throws(() => bindExportToken(t, { streamId: 'other' }),
    (e) => e.code === 'export_stream_mismatch');
  assert.doesNotThrow(() => bindExportToken(t, { streamId: 'st1' }));
});

test('分片游标: 签发/解析往返', () => {
  const x = {
    id: 'exp1', streamId: 'st1', snapshotSeq: 42, workerCount: 4,
    workerIndex: 2, fromTs: EXP.fromTs, toTs: EXP.toTs, pageSize: 25,
    after: { bucket: 3, ts: '2026-01-01T00:00:00Z', sampleId: 'z' },
  };
  const c = issueShardCursor('sec', x);
  const got = parseShardCursor('sec', c);
  assert.deepEqual(got, x);
  // 首页 after=null
  const first = issueShardCursor('sec', { ...x, after: null });
  assert.equal(parseShardCursor('sec', first).after, null);
});

test('分片游标: 篡改 -> shard_cursor_signature_invalid', () => {
  const c = issueShardCursor('sec', {
    ...EXP, workerIndex: 0, pageSize: 10, after: null,
  });
  const mid = Math.floor(c.length / 2);
  const flipped = c.slice(0, mid) + (c[mid] === 'A' ? 'B' : 'A') + c.slice(mid + 1);
  assert.throws(() => parseShardCursor('sec', flipped),
    (e) => e.code === 'shard_cursor_signature_invalid');
});

test('分片游标: 只能用于签发它的那个分片', () => {
  const token = parseExportToken('sec', issueExportToken('sec', EXP));
  const cur = parseShardCursor('sec', issueShardCursor('sec', {
    ...EXP, workerIndex: 1, pageSize: 10,
    after: { bucket: 1, ts: '2026-01-01T00:00:00Z', sampleId: 'q' },
  }));
  // 正确绑定
  assert.doesNotThrow(() =>
    bindShardCursor(cur, { token, streamId: 'st1', workerIndex: 1, pageSize: 10 }));
  // 换 workerIndex（游标用于另一分片）
  assert.throws(
    () => bindShardCursor(cur, { token, streamId: 'st1', workerIndex: 0, pageSize: 10 }),
    (e) => e.code === 'shard_cursor_mismatch');
  // 跨流
  assert.throws(
    () => bindShardCursor(cur, { token, streamId: 'stX', workerIndex: 1, pageSize: 10 }),
    (e) => e.code === 'shard_stream_mismatch');
  // 另一次导出（不同 id）
  const token2 = parseExportToken('sec', issueExportToken('sec', { ...EXP, id: 'exp2' }));
  assert.throws(
    () => bindShardCursor(cur, { token: token2, streamId: 'st1', workerIndex: 1, pageSize: 10 }),
    (e) => e.code === 'shard_cursor_mismatch');
  // worker 数不同的导出
  const token3 = parseExportToken('sec', issueExportToken('sec', { ...EXP, workerCount: 8 }));
  assert.throws(
    () => bindShardCursor(cur, { token: token3, streamId: 'st1', workerIndex: 1, pageSize: 10 }),
    (e) => e.code === 'shard_cursor_mismatch');
});

test('普通分页游标通道拒绝 v2 分片游标', () => {
  const c = issueShardCursor('sec', {
    ...EXP, workerIndex: 0, pageSize: 10, after: null,
  });
  assert.throws(() => parseCursor('sec', c),
    (e) => e.code === 'export_token_required');
});

/* ============================ 并行导出：创建请求校验 ============================ */

test('校验: 导出创建 workerCount 2..8 与时间范围', () => {
  assert.deepEqual(validateExport({ workerCount: 2 }).workerCount, 2);
  assert.deepEqual(validateExport({ workerCount: 8 }).workerCount, 8);
  for (const bad of [undefined, null, 1, 9, 0, -1, 2.5, '4', true]) {
    assert.throws(() => validateExport({ workerCount: bad }), ValidationError,
      `workerCount=${JSON.stringify(bad)}`);
  }
  // from/to 可缺省（全时间范围）
  assert.doesNotThrow(() => validateExport({ workerCount: 4 }));
  const r = validateExport({
    from: '2026-01-01T08:00:00+08:00', to: '2026-01-02T00:00:00Z', workerCount: 3,
  });
  assert.equal(r.fromTs, '2026-01-01T00:00:00.000000000Z');
  assert.equal(r.workerCount, 3);
  assert.throws(() => validateExport({
    from: '2026-02-01T00:00:00Z', to: '2026-01-01T00:00:00Z', workerCount: 2,
  }), ValidationError);
  assert.throws(() => validateExport(null), ValidationError);
});

test('校验: 查询参数 exportToken / workerIndex', () => {
  const q1 = validateQuery(new URLSearchParams('exportToken=t&workerIndex=0'));
  assert.equal(q1.exportToken, 't');
  assert.equal(q1.workerIndex, 0);
  const q2 = validateQuery(new URLSearchParams(''));
  assert.equal(q2.exportToken, null);
  assert.equal(q2.workerIndex, null);
  assert.throws(
    () => validateQuery(new URLSearchParams('exportToken=t&workerIndex=-1')),
    ValidationError);
  assert.throws(
    () => validateQuery(new URLSearchParams('exportToken=t&workerIndex=abc')),
    ValidationError);
});
