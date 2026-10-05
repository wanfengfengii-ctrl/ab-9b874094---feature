import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { createApp } from '../src/app.js';
import { issueCursor } from '../src/cursor.js';

const SECRET = 'export-http-fixed-secret';
let dir;
let baseUrl;
let app;

async function listen(a) {
  await new Promise((resolve) => a.server.listen(0, '127.0.0.1', resolve));
  return `http://127.0.0.1:${a.server.address().port}`;
}

before(async () => {
  dir = mkdtempSync(join(tmpdir(), 'obs-exp-'));
  app = createApp({ dbPath: join(dir, 'exp.db'), cursorSecret: SECRET });
  baseUrl = await listen(app);
});

after(async () => {
  await app.close();
  rmSync(dir, { recursive: true, force: true });
});

const api = (path, init) => fetch(baseUrl + path, init);
const postSamples = (stream, samples) =>
  api(`/api/streams/${stream}/samples`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ samples }),
  });
const createExport = (stream, body) =>
  api(`/api/streams/${stream}/exports`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: body === undefined ? '' : JSON.stringify(body),
  });

/** 并发翻完一个导出的全部分片，返回 { items, responses }。 */
async function drainExport(exportToken, workerCount, { pageSize = 7, stream } = {}) {
  const items = [];
  const responses = [];
  async function worker(workerIndex) {
    let url =
      `/api/streams/${stream}/samples?exportToken=${encodeURIComponent(exportToken)}` +
      `&workerIndex=${workerIndex}&pageSize=${pageSize}`;
    let guard = 0;
    for (;;) {
      const r = await api(url);
      assert.equal(r.status, 200, `worker ${workerIndex} 请求失败: ${r.status}`);
      const page = await r.json();
      responses.push(page);
      for (const it of page.items) items.push(it);
      assert.equal(page.workerIndex, workerIndex);
      assert.equal(page.workerCount, workerCount);
      if (page.done) {
        assert.equal(page.nextCursor, null);
        return;
      }
      url =
        `/api/streams/${stream}/samples?exportToken=${encodeURIComponent(exportToken)}` +
        `&workerIndex=${workerIndex}&pageSize=${pageSize}` +
        `&cursor=${encodeURIComponent(page.nextCursor)}`;
      assert.ok(++guard < 50, '分页守卫');
    }
  }
  await Promise.all(Array.from({ length: workerCount }, (_, w) => worker(w)));
  return { items, responses };
}

test('POST exports: 创建返回不透明令牌、快照序号与实际工作进程数', async () => {
  await postSamples('create', [
    { sampleId: 'a', ts: '2026-01-01T00:00:00Z', value: 1 },
  ]);

  // 空请求体 -> 默认 workerCount=4
  let r = await createExport('create');
  assert.equal(r.status, 201);
  let j = await r.json();
  assert.equal(typeof j.exportToken, 'string');
  assert.ok(j.exportToken.includes('.'));
  assert.equal(j.workerCount, 4);
  assert.equal(j.snapshotSeq, 1);
  // 不透明：明文不得泄露流名 / 快照参数
  const decoded = JSON.parse(
    Buffer.from(j.exportToken.split('.')[0], 'base64url').toString('utf8')
  );
  assert.deepEqual(Object.keys(decoded).sort(), ['d', 'v']);

  // 显式 workerCount + 时间范围
  r = await createExport('create', {
    workerCount: 2,
    from: '2026-01-01T00:00:00Z',
    to: '2026-12-31T23:59:59Z',
  });
  assert.equal(r.status, 201);
  j = await r.json();
  assert.equal(j.workerCount, 2);
  assert.equal(j.snapshotSeq, 1);

  // workerCount 越界 / 非法
  for (const bad of [1, 9, 0, -3, 2.5, '4', true]) {
    r = await createExport('create', { workerCount: bad });
    assert.equal(r.status, 400, `workerCount=${JSON.stringify(bad)} 应 400`);
    const body = await r.json();
    assert.equal(body.error.code, 'invalid_request');
  }
  // 坏 JSON / 非对象体
  r = await fetch(`${baseUrl}/api/streams/create/exports`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: '{oops',
  });
  assert.equal(r.status, 400);
  r = await createExport('create', [1, 2, 3]);
  assert.equal(r.status, 400);
});

test('GET exports 仅支持 POST；路由不影响既有 /samples', async () => {
  let r = await api('/api/streams/create/exports');
  assert.equal(r.status, 405);
  r = await api('/api/streams/create/samples');
  assert.equal(r.status, 200, '旧 GET samples 行为不变');
  r = await api('/api/streams/create/unknown');
  assert.equal(r.status, 404);
});

test('并行导出：8 个并发分片不重不漏恰好覆盖创建时快照，期间新增数据不进入', async () => {
  const stream = 'parallel';
  const N = 57;
  // 乱序批量写入
  const items = Array.from({ length: N }, (_, i) => ({
    sampleId: `n${String(i).padStart(3, '0')}`,
    ts: `2026-05-01T${String(Math.floor(i / 60)).padStart(2, '0')}:${String(i % 60).padStart(2, '0')}:00Z`,
    value: i,
  })).sort(() => Math.random() - 0.5);
  for (let i = 0; i < items.length; i += 13) {
    const pr = await postSamples(stream, items.slice(i, i + 13));
    assert.equal(pr.status, 200);
  }

  // 快照中应有稳定序
  const expectedIds = Array.from({ length: N }, (_, i) => `n${String(i).padStart(3, '0')}`)
    .sort((a, b) => {
      const ia = Number(a.slice(1));
      const ib = Number(b.slice(1));
      return ia - ib; // ts 随 i 单调
    });

  const created = await (await createExport(stream, { workerCount: 8 })).json();
  const snapshotSeq = created.snapshotSeq;

  // 创建导出之后：追加更早 + 更晚时刻的数据
  const late = await postSamples(stream, [
    { sampleId: 'EARLY', ts: '2001-01-01T00:00:00Z', value: -1 },
    { sampleId: 'FUTURE', ts: '2099-01-01T00:00:00Z', value: 999 },
  ]);
  assert.equal(late.status, 200);

  const { items: got, responses } = await drainExport(
    created.exportToken, 8, { pageSize: 7, stream }
  );

  // 全部响应都钉在创建时快照
  assert.ok(responses.every((p) => p.snapshotSeq === snapshotSeq));

  const ids = got.map((x) => x.sampleId);
  assert.equal(ids.length, N, '恰好 N 条');
  assert.equal(new Set(ids).size, N, '无重复（分片不重叠）');
  assert.deepEqual(ids.sort(), expectedIds.sort(), '并集等于创建时快照全部观测');
  assert.ok(!ids.includes('EARLY') && !ids.includes('FUTURE'), '期间新增数据不进入');

  // 每分片内部按 (ts, sampleId) 稳定有序
  for (const p of responses) {
    const keys = p.items.map((it) => it.ts + '/' + it.sampleId);
    assert.deepEqual(keys, [...keys].sort());
  }

  // 重放：再翻一遍必须得到同样结果（令牌可重复驱动完成）
  const again = await drainExport(created.exportToken, 8, { pageSize: 3, stream });
  assert.deepEqual(again.items.map((x) => x.sampleId).sort(), expectedIds.sort());
});

test('并行导出：翻页省略 pageSize 时沿用首页页大小；每分片内部稳定有序', async () => {
  const stream = 'pagesize';
  await postSamples(stream, Array.from({ length: 12 }, (_, i) => ({
    sampleId: `p${i}`,
    ts: `2026-07-01T00:00:${String(i).padStart(2, '0')}Z`,
    value: i,
  })));
  const created = await (await createExport(stream, { workerCount: 3 })).json();

  // 首页 pageSize=2；第二页故意省略 pageSize，应仍按 2 取
  const first = await (await api(
    `/api/streams/${stream}/samples?exportToken=${encodeURIComponent(created.exportToken)}` +
    `&workerIndex=0&pageSize=2`
  )).json();
  assert.equal(first.items.length, 2);
  assert.equal(first.pageSize, 2);
  const second = await (await api(
    `/api/streams/${stream}/samples?exportToken=${encodeURIComponent(created.exportToken)}` +
    `&workerIndex=0&cursor=${encodeURIComponent(first.nextCursor)}`
  )).json();
  assert.equal(second.pageSize, 2, '省略 pageSize 沿用首页页大小');
  assert.equal(second.items.length, 2);
  // 两页之间无重叠
  assert.equal(
    new Set([...first.items, ...second.items].map((i) => i.sampleId)).size,
    4
  );
});

test('并行导出：时间范围在创建时固定', async () => {  const stream = 'ranged';
  await postSamples(stream, [
    { sampleId: 'jan', ts: '2026-01-15T00:00:00Z', value: 1 },
    { sampleId: 'feb', ts: '2026-02-15T00:00:00Z', value: 2 },
    { sampleId: 'mar', ts: '2026-03-15T00:00:00Z', value: 3 },
  ]);
  const created = await (await createExport(stream, {
    workerCount: 2,
    from: '2026-02-01T00:00:00Z',
    to: '2026-02-28T23:59:59Z',
  })).json();
  const { items } = await drainExport(created.exportToken, 2, { pageSize: 10, stream });
  assert.deepEqual(items.map((i) => i.sampleId), ['feb']);

  // 携带令牌又试图改时间范围 -> export_range_mismatch（创建时范围不可变）
  let r = await api(
    `/api/streams/${stream}/samples?exportToken=${encodeURIComponent(created.exportToken)}` +
    `&workerIndex=0&from=2026-01-01T00:00:00Z`
  );
  assert.equal(r.status, 400);
  assert.equal((await r.json()).error.code, 'export_range_mismatch');
  // 与创建时一致的 from/to 显式给出则无妨
  r = await api(
    `/api/streams/${stream}/samples?exportToken=${encodeURIComponent(created.exportToken)}` +
    `&workerIndex=0&from=2026-02-01T00:00:00Z&to=2026-02-28T23:59:59Z`
  );
  assert.equal(r.status, 200);
});

test('并行导出：空分片立即结束（数据条数少于 workerCount）', async () => {  const stream = 'sparse';
  await postSamples(stream, [
    { sampleId: 'a', ts: '2026-01-01T00:00:00Z', value: 1 },
  ]);
  const created = await (await createExport(stream, { workerCount: 8 })).json();
  const { items, responses } = await drainExport(
    created.exportToken, 8, { pageSize: 10, stream }
  );
  assert.deepEqual(items.map((i) => i.sampleId), ['a']);
  // 7 个空分片首请求即 done=true 且无游标
  const emptyDone = responses.filter((p) => p.items.length === 0);
  assert.equal(emptyDone.length, 7);
  assert.ok(emptyDone.every((p) => p.done && p.nextCursor === null));
});

test('导出错误：令牌篡改 / 伪造 / 跨流 / 工作进程越界，错误码可区分且不泄露数据', async () => {
  const stream = 'errors';
  await postSamples(stream, Array.from({ length: 20 }, (_, i) =>
    ({ sampleId: `e${i}`, ts: `2026-04-01T00:00:${String(i).padStart(2, '0')}Z`, value: i })));
  const created = await (await createExport(stream, { workerCount: 4 })).json();
  const tok = created.exportToken;
  const samples = (qs) => `/api/streams/${stream}/samples?${qs}`;

  const expectError = async (pathOrInit, status, code) => {
    const r = typeof pathOrInit === 'string'
      ? await api(pathOrInit)
      : await fetch(baseUrl + pathOrInit.url, pathOrInit.init);
    assert.equal(r.status, status, `期望 ${status}，实际 ${r.status}`);
    const body = await r.json();
    assert.equal(body.error.code, code);
    assert.equal(Array.isArray(body.items), false, '错误响应不得携带数据');
  };

  // 垃圾 / 篡改 / 他密钥签发
  await expectError(samples(`exportToken=garbage&workerIndex=0`), 400, 'invalid_export_token');
  const [bodyB64, sig] = tok.split('.');
  const tampered = bodyB64 + '.' + sig.slice(0, -1) + (sig.endsWith('A') ? 'B' : 'A');
  await expectError(
    samples(`exportToken=${encodeURIComponent(tampered)}&workerIndex=0`),
    400, 'export_token_invalid'
  );
  const foreign = issueCursor('another-secret', { v: 2, d: 'x' });
  await expectError(
    samples(`exportToken=${encodeURIComponent(foreign)}&workerIndex=0`),
    400, 'export_token_invalid'
  );

  // 签名合法但会话不存在 -> 404（不区分“从未存在”，避免探测）
  const ghost = issueCursor(SECRET, { v: 2, d: 'does-not-exist' });
  await expectError(
    samples(`exportToken=${encodeURIComponent(ghost)}&workerIndex=0`),
    404, 'export_not_found'
  );

  // 跨流使用
  {
    const r = await api(
      `/api/streams/OTHER/samples?exportToken=${encodeURIComponent(tok)}&workerIndex=0`
    );
    assert.equal(r.status, 400);
    assert.equal((await r.json()).error.code, 'export_stream_mismatch');
  }

  // workerIndex 越界（本导出仅 4 个进程）
  await expectError(samples(`exportToken=${encodeURIComponent(tok)}&workerIndex=4`),
    400, 'worker_index_out_of_range');
  await expectError(samples(`exportToken=${encodeURIComponent(tok)}&workerIndex=99`),
    400, 'invalid_query');
  // 缺 workerIndex
  await expectError(samples(`exportToken=${encodeURIComponent(tok)}`),
    400, 'invalid_query');

  // 把普通分页游标当作 exportToken
  const normalPage = await (await api(
    `/api/streams/${stream}/samples?pageSize=2`
  )).json();
  await expectError(
    samples(`exportToken=${encodeURIComponent(normalPage.nextCursor)}&workerIndex=0`),
    400, 'invalid_export_token'
  );
});

test('导出错误：分片游标串到其他分片 / 其他导出 / 无令牌普通请求', async () => {
  const stream = 'cursorerr';
  await postSamples(stream, Array.from({ length: 30 }, (_, i) =>
    ({ sampleId: `c${String(i).padStart(2, '0')}`, ts: `2026-06-01T00:00:${String(i).padStart(2, '0')}Z`, value: i })));
  const ex1 = await (await createExport(stream, { workerCount: 4 })).json();
  const ex2 = await (await createExport(stream, { workerCount: 4 })).json();

  // worker 1 取首页，拿到属于分片 1 的游标
  const page1 = await (await api(
    `/api/streams/${stream}/samples?exportToken=${encodeURIComponent(ex1.exportToken)}` +
    `&workerIndex=1&pageSize=2`
  )).json();
  assert.ok(page1.nextCursor);
  const shardCursor = page1.nextCursor;

  const expectCode = async (qs, code, status = 400) => {
    const r = await api(`/api/streams/${stream}/samples?${qs}`);
    assert.equal(r.status, status);
    assert.equal((await r.json()).error.code, code);
  };

  // 同一导出、不同工作进程 -> cursor_worker_mismatch
  await expectCode(
    `exportToken=${encodeURIComponent(ex1.exportToken)}&workerIndex=2` +
    `&pageSize=2&cursor=${encodeURIComponent(shardCursor)}`,
    'cursor_worker_mismatch'
  );
  // 游标来自另一导出会话 -> cursor_export_mismatch
  await expectCode(
    `exportToken=${encodeURIComponent(ex2.exportToken)}&workerIndex=1` +
    `&pageSize=2&cursor=${encodeURIComponent(shardCursor)}`,
    'cursor_export_mismatch'
  );
  // 跨流：把整条请求（令牌+分片游标）搬到另一流，先报令牌跨流
  {
    const r = await api(
      `/api/streams/OTHER2/samples?exportToken=${encodeURIComponent(ex1.exportToken)}` +
      `&workerIndex=1&cursor=${encodeURIComponent(shardCursor)}`
    );
    assert.equal(r.status, 400);
    assert.equal((await r.json()).error.code, 'export_stream_mismatch');
  }
  // 分片游标脱离 exportToken 走普通分页 -> invalid_cursor
  await expectCode(`pageSize=2&cursor=${encodeURIComponent(shardCursor)}`, 'invalid_cursor');
  // 普通分页游标混进导出请求 -> invalid_cursor
  const normal = await (await api(`/api/streams/${stream}/samples?pageSize=2`)).json();
  await expectCode(
    `exportToken=${encodeURIComponent(ex1.exportToken)}&workerIndex=0` +
    `&pageSize=2&cursor=${encodeURIComponent(normal.nextCursor)}`,
    'invalid_cursor'
  );
  // 篡改分片游标 -> cursor_signature_invalid
  const t = shardCursor.slice(0, -1) +
    (shardCursor.endsWith('A') ? 'B' : 'A');
  await expectCode(
    `exportToken=${encodeURIComponent(ex1.exportToken)}&workerIndex=1` +
    `&pageSize=2&cursor=${encodeURIComponent(t)}`,
    'cursor_signature_invalid'
  );
});

test('导出令牌与分片游标在服务重启后仍能让各工作进程完成原快照', async () => {
  const stream = 'export-restart';
  await postSamples(stream, Array.from({ length: 25 }, (_, i) => ({
    sampleId: `r${String(i).padStart(2, '0')}`,
    ts: `2026-08-01T00:00:${String(i).padStart(2, '0')}Z`,
    value: i,
  })));
  const created = await (await createExport(stream, { workerCount: 5 })).json();
  const snapshotSeq = created.snapshotSeq;

  // worker 0、2 各翻一部分后停住（持有游标）
  async function firstPage(workerIndex, pageSize) {
    const r = await api(
      `/api/streams/${stream}/samples?exportToken=${encodeURIComponent(created.exportToken)}` +
      `&workerIndex=${workerIndex}&pageSize=${pageSize}`
    );
    return r.json();
  }
  const p0 = await firstPage(0, 2);
  const p2 = await firstPage(2, 3);
  const held = [
    { w: 0, items: p0.items, cursor: p0.nextCursor },
    { w: 2, items: p2.items, cursor: p2.nextCursor },
  ];
  assert.ok(held[0].cursor && held[1].cursor);

  // 重启后再新增数据
  await app.close();
  app = createApp({ dbPath: join(dir, 'exp.db'), cursorSecret: SECRET });
  baseUrl = await listen(app);
  const late = await postSamples(stream, [
    { sampleId: 'AFTER_RESTART_EARLY', ts: '2000-01-01T00:00:00Z', value: 0 },
  ]);
  assert.equal(late.status, 200);

  // 全部 5 个分片完成；0/2 从持有游标继续
  const merged = [];
  await Promise.all(Array.from({ length: 5 }, async (_, w) => {
    let first = true;
    let url =
      `/api/streams/${stream}/samples?exportToken=${encodeURIComponent(created.exportToken)}` +
      `&workerIndex=${w}&pageSize=4`;
    const h = held.find((x) => x.w === w);
    if (h) {
      merged.push(...h.items);
      url += `&cursor=${encodeURIComponent(h.cursor)}`;
      first = false;
    }
    let guard = 0;
    for (;;) {
      const r = await api(url);
      assert.equal(r.status, 200);
      const page = await r.json();
      assert.equal(page.snapshotSeq, snapshotSeq, '重启后快照序号不变');
      merged.push(...page.items);
      if (page.done) return;
      url =
        `/api/streams/${stream}/samples?exportToken=${encodeURIComponent(created.exportToken)}` +
        `&workerIndex=${w}&pageSize=4&cursor=${encodeURIComponent(page.nextCursor)}`;
      first = false;
      assert.ok(++guard < 20);
    }
  }));

  const ids = merged.map((m) => m.sampleId);
  assert.equal(ids.length, 25);
  assert.equal(new Set(ids).size, 25);
  assert.ok(!ids.includes('AFTER_RESTART_EARLY'));
  assert.deepEqual(
    ids.sort(),
    Array.from({ length: 25 }, (_, i) => `r${String(i).padStart(2, '0')}`)
  );
});
