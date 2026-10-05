import { test } from 'node:test';
import assert from 'node:assert/strict';

import { Store } from '../src/db.js';
import {
  issueCursor,
  parseCursor,
  bindCursor,
  bindExportToken,
  bindShardCursor,
  CursorError,
} from '../src/cursor.js';
import {
  validateExportCreate,
  validateQuery,
  ValidationError,
  EXPORT_WORKER_BOUNDS,
  TIME_BOUNDS,
} from '../src/validation.js';

const R = (ts, sid, v = 1) => ({ sampleId: sid, ts, value: v });
const SECRET = 'export-unit-secret';

function drainShard(store, rec, workerIndex, pageSize = 2) {
  const out = [];
  let after = null;
  for (;;) {
    const rows = store.readShardPage(
      { ...rec, workerIndex }, after, pageSize + 1
    );
    const hasMore = rows.length > pageSize;
    const page = hasMore ? rows.slice(0, pageSize) : rows;
    out.push(...page);
    if (!hasMore) break;
    const last = page[page.length - 1];
    after = { ts: last.ts, sampleId: last.sampleId };
  }
  return out;
}

test('Store.readShardPage: 各分片互不重叠，合并恰好覆盖快照全部', () => {
  const s = new Store(':memory:');
  // 乱序、同时刻写入 20 条
  const items = [];
  for (let i = 0; i < 20; i++) {
    items.push(R(`2026-01-01T00:00:${String(i % 60).padStart(2, '0')}Z`, `s${i}`));
  }
  for (let i = items.length - 1; i > 0; i--) {
    const k = (i * 7919) % (i + 1);
    [items[i], items[k]] = [items[k], items[i]];
  }
  // 分批接入
  s.insertBatch('st', items.slice(0, 7));
  s.insertBatch('st', items.slice(7));
  const rec = {
    streamId: 'st',
    snapshotSeq: s.currentSeq(),
    fromTs: TIME_BOUNDS.MIN_TS,
    toTs: TIME_BOUNDS.MAX_TS,
    workerCount: 6,
  };

  const merged = [];
  for (let w = 0; w < 6; w++) merged.push(...drainShard(s, rec, w, 3));

  // 合并结果按分片顺序拼接；按 (ts, sampleId) 重排后应与普通快照分页完全一致
  const key = (r) => `${r.ts}/${r.sampleId}`;
  const mergedSorted = merged.map(key).sort();
  const expected = s.readPage(
    'st', rec.snapshotSeq, { fromTs: rec.fromTs, toTs: rec.toTs }, null, 1000
  ).map(key);
  assert.deepEqual(mergedSorted, expected, '各分片并集恰好等于快照全部元素');
  assert.equal(merged.length, 20);
  assert.equal(new Set(merged.map((r) => r.sampleId)).size, 20, '无重复');

  // 每个分片内部也是稳定升序
  for (let w = 0; w < 6; w++) {
    const rows = drainShard(s, rec, w, 5);
    const keys = rows.map(key);
    assert.deepEqual(keys, [...keys].sort(), `分片 ${w} 内部稳定有序`);
  }
  s.close();
});

test('Store.readShardPage: 快照固定后新增数据（含更早时刻）不进入任何分片；空分片存在', () => {
  const s = new Store(':memory:');
  s.insertBatch('st', [
    R('2026-01-01T00:00:00Z', 'a'),
    R('2026-01-01T00:00:05Z', 'b'),
    R('2026-01-01T00:00:10Z', 'c'),
  ]);
  const rec = {
    streamId: 'st', snapshotSeq: s.currentSeq(),
    fromTs: TIME_BOUNDS.MIN_TS, toTs: TIME_BOUNDS.MAX_TS, workerCount: 8,
  };
  s.insertBatch('st', [
    R('2025-01-01T00:00:00Z', 'early'),
    R('2027-01-01T00:00:00Z', 'late'),
  ]);

  const counts = [];
  const merged = [];
  for (let w = 0; w < 8; w++) {
    const rows = store0(s, rec, w);
    counts.push(rows.length);
    merged.push(...rows);
  }
  assert.deepEqual(merged.map((r) => r.sampleId).sort(), ['a', 'b', 'c']);
  assert.equal(counts.filter((n) => n === 0).length, 5, '8 个分片里 5 个为空');

  // 空流：所有分片立即结束
  const empty = {
    streamId: 'nobody', snapshotSeq: s.currentSeq(),
    fromTs: TIME_BOUNDS.MIN_TS, toTs: TIME_BOUNDS.MAX_TS, workerCount: 4,
  };
  for (let w = 0; w < 4; w++) assert.equal(store0(s, empty, w).length, 0);
  s.close();
});

function store0(s, rec, w) {
  return s.readShardPage({ ...rec, workerIndex: w }, null, 100);
}

test('Store: 导出会话持久化，重新打开数据库后仍可取回', () => {
  // node:sqlite 无法在同进程对同一文件开两个连接，这里只验证写入/读取往返；
  // 跨重启场景由 HTTP 集成测试覆盖。
  const s = new Store(':memory:');
  s.createExport({
    tokenId: 'tok-1', streamId: 'st', snapshotSeq: 11,
    fromTs: '2026-01-01T00:00:00.000000000Z',
    toTs: '2026-02-01T00:00:00.000000000Z',
    workerCount: 3, createdAt: '2026-10-05T00:00:00.000Z',
  });
  const got = s.getExport('tok-1');
  assert.deepEqual(got, {
    tokenId: 'tok-1', streamId: 'st', snapshotSeq: 11,
    fromTs: '2026-01-01T00:00:00.000000000Z',
    toTs: '2026-02-01T00:00:00.000000000Z',
    workerCount: 3, createdAt: '2026-10-05T00:00:00.000Z',
  });
  assert.equal(s.getExport('missing'), null);
  s.close();
});

test('游标: 导出令牌 v2 签发/解析/篡改', () => {
  const token = issueCursor(SECRET, { v: 2, d: 'opaque-id' });
  assert.deepEqual(parseCursor(SECRET, token), { v: 2, d: 'opaque-id' });

  const [body, sig] = token.split('.');
  assert.throws(
    () => parseCursor(SECRET, body + '.' + sig.replace(/./, 'X')),
    (e) => e instanceof CursorError && e.code === 'cursor_signature_invalid'
  );
  assert.throws(
    () => parseCursor(SECRET, 'no-dot'),
    (e) => e instanceof CursorError && e.code === 'invalid_cursor'
  );
  // 缺 id / v 非法
  const bad = issueCursor(SECRET, { v: 2 });
  assert.throws(() => parseCursor(SECRET, bad),
    (e) => e instanceof CursorError && e.code === 'invalid_cursor');
});

test('游标: bindExportToken 未找到 404、跨流拒绝、类型不符拒绝', () => {
  const rec = {
    tokenId: 't1', streamId: 's1', snapshotSeq: 5,
    fromTs: 'f', toTs: 't', workerCount: 3,
  };
  const load = (id) => (id === 't1' ? rec : null);

  assert.equal(
    bindExportToken({ v: 2, d: 't1' }, { streamId: 's1' }, load),
    rec
  );

  assert.throws(
    () => bindExportToken({ v: 2, d: 'nope' }, { streamId: 's1' }, load),
    (e) => e instanceof CursorError && e.code === 'export_not_found' &&
          e.statusCode === 404
  );
  assert.throws(
    () => bindExportToken({ v: 2, d: 't1' }, { streamId: 's2' }, load),
    (e) => e instanceof CursorError && e.code === 'export_stream_mismatch'
  );
  // 把普通分页游标当导出令牌
  const v1 = { v: 1, s: 's1', q: 1, f: 'f', t: 't', p: 10, a: null };
  assert.throws(
    () => bindExportToken(v1, { streamId: 's1' }, load),
    (e) => e instanceof CursorError && e.code === 'invalid_export_token'
  );
});

test('游标: bindShardCursor 仅接受同一导出的同一分片游标', () => {
  const rec = {
    tokenId: 't1', streamId: 's1', snapshotSeq: 9,
    fromTs: 'f', toTs: 't', workerCount: 4,
  };
  const ctx = {
    exportRec: rec, tokenId: 't1', streamId: 's1',
    workerIndex: 2, pageSize: 10,
  };
  const shard = (over = {}) => ({
    v: 1, s: 's1', q: 9, f: 'f', t: 't', p: 10,
    a: { ts: 'x', i: 'y' }, e: 't1', w: 2, ...over,
  });

  const bound = bindShardCursor(shard(), ctx);
  assert.equal(bound.snapshotSeq, 9);
  assert.deepEqual(bound.after, { ts: 'x', sampleId: 'y' });

  // 不是分片游标（普通 v1）
  assert.throws(
    () => bindShardCursor({ v: 1, s: 's1', q: 9, f: 'f', t: 't', p: 10, a: null }, ctx),
    (e) => e.code === 'invalid_cursor'
  );
  // 属于其他导出会话
  assert.throws(
    () => bindShardCursor(shard({ e: 't-other' }), ctx),
    (e) => e.code === 'cursor_export_mismatch'
  );
  // 属于其他分片（把分片游标串到另一工作进程）
  assert.throws(
    () => bindShardCursor(shard({ w: 0 }), ctx),
    (e) => e.code === 'cursor_worker_mismatch'
  );
  // 跨流
  assert.throws(
    () => bindShardCursor(shard({ s: 's-other' }), { ...ctx, streamId: 's-other' }),
    (e) => e.code === 'cursor_stream_mismatch'
  );
  // 游标快照参数与导出记录不符（正常签名下不会发生，篡改存储才可能）
  assert.throws(
    () => bindShardCursor(shard({ q: 8 }), ctx),
    (e) => e.code === 'cursor_export_mismatch'
  );
});

test('游标: 普通 bindCursor 拒绝分片游标（分片游标不能脱离 exportToken 使用）', () => {
  const shard = {
    v: 1, s: 's1', q: 9, f: 'f', t: 't', p: 10,
    a: null, e: 't1', w: 0,
  };
  assert.throws(
    () => bindCursor(shard, { streamId: 's1', fromTs: null, toTs: null, pageSize: null }),
    (e) => e instanceof CursorError && e.code === 'invalid_cursor'
  );
});

test('校验: validateExportCreate 默认/边界/非法值/时间范围', () => {
  assert.deepEqual(validateExportCreate({}),
    { fromTs: TIME_BOUNDS.MIN_TS, toTs: TIME_BOUNDS.MAX_TS, workerCount: null });
  assert.equal(validateExportCreate({ workerCount: 2 }).workerCount, 2);
  assert.equal(validateExportCreate({ workerCount: 8 }).workerCount, 8);
  assert.equal(EXPORT_WORKER_BOUNDS.MIN, 2);
  assert.equal(EXPORT_WORKER_BOUNDS.MAX, 8);

  for (const bad of [1, 9, 0, -1, 2.5, '4', true, null]) {
    // null 表示缺省（合法），其余非法
    if (bad === null) continue;
    assert.throws(() => validateExportCreate({ workerCount: bad }), ValidationError,
      `workerCount=${JSON.stringify(bad)} 应拒绝`);
  }

  const r = validateExportCreate({
    from: '2026-01-01T08:00:00+08:00',
    to: '2026-01-02T00:00:00Z',
    workerCount: 3,
  });
  assert.equal(r.fromTs, '2026-01-01T00:00:00.000000000Z');
  assert.equal(r.workerCount, 3);

  assert.throws(
    () => validateExportCreate({ from: '2026-02-01T00:00:00Z', to: '2026-01-01T00:00:00Z' }),
    ValidationError
  );
  assert.throws(() => validateExportCreate({ from: 'not-a-time' }), ValidationError);
  assert.throws(() => validateExportCreate('nope'), ValidationError);
  assert.throws(() => validateExportCreate([]), ValidationError);
});

test('校验: GET 携带 exportToken 时必须带零基 workerIndex', () => {
  const token = issueCursor(SECRET, { v: 2, d: 'x' });
  const q = validateQuery(new URLSearchParams(
    `exportToken=${encodeURIComponent(token)}&workerIndex=3&pageSize=50`
  ));
  assert.equal(q.workerIndex, 3);
  assert.equal(q.exportToken, token);
  assert.equal(q.pageSize, 50);

  assert.throws(
    () => validateQuery(new URLSearchParams(`exportToken=${encodeURIComponent(token)}`)),
    ValidationError, '缺 workerIndex'
  );
  assert.throws(
    () => validateQuery(new URLSearchParams(
      `exportToken=${encodeURIComponent(token)}&workerIndex=-1`)),
    ValidationError
  );
  assert.throws(
    () => validateQuery(new URLSearchParams(
      `exportToken=${encodeURIComponent(token)}&workerIndex=abc`)),
    ValidationError
  );
  assert.throws(
    () => validateQuery(new URLSearchParams(
      `exportToken=${encodeURIComponent(token)}&workerIndex=8`)),
    ValidationError, 'workerIndex 上限 7（workerCount 最大 8）'
  );

  // 不携带 exportToken：workerIndex 被忽略，旧行为完全兼容
  const legacy = validateQuery(new URLSearchParams('pageSize=10'));
  assert.equal(legacy.exportToken, null);
  assert.equal(legacy.workerIndex, null);
});
