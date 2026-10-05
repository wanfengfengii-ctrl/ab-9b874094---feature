import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { createApp } from '../src/app.js';

const SECRET = 'integration-fixed-secret';
let dir;
let baseUrl;
let app;

async function listen(a) {
  await new Promise((resolve) => a.server.listen(0, '127.0.0.1', resolve));
  const { port } = a.server.address();
  return `http://127.0.0.1:${port}`;
}

before(async () => {
  dir = mkdtempSync(join(tmpdir(), 'obs-int-'));
  app = createApp({ dbPath: join(dir, 'int.db'), cursorSecret: SECRET });
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
const createExport = (stream, body = {}) =>
  api(`/api/streams/${stream}/exports`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });

/** 并发排空某导出的全部分片，返回按 worker 收集的 sampleId 数组。 */
async function drainExport(stream, token, workerCount, { pageSize = 7 } = {}) {
  async function drainOne(w) {
    const ids = [];
    let cursor = null;
    let guard = 0;
    do {
      const u = `/api/streams/${stream}/samples?exportToken=${encodeURIComponent(token)}` +
        `&workerIndex=${w}&pageSize=${pageSize}` +
        (cursor ? `&cursor=${encodeURIComponent(cursor)}` : '');
      const r = await api(u);
      if (!r.ok) {
        const j = await r.json().catch(() => ({}));
        throw new Error(`worker ${w} -> ${r.status} ${j.error?.code ?? ''}`);
      }
      const page = await r.json();
      for (const it of page.items) ids.push(it.sampleId);
      cursor = page.nextCursor;
      if (++guard > 100) throw new Error('分页守卫触发');
    } while (cursor);
    return ids;
  }
  // 故意并发：多工作进程同时翻页
  return Promise.all(Array.from({ length: workerCount }, (_, w) => drainOne(w)));
}

test('健康检查可用', async () => {
  const r = await api('/health');
  assert.equal(r.status, 200);
  const j = await r.json();
  assert.equal(j.status, 'ok');
});

test('POST 原子批：成功与 409 整批拒绝', async () => {
  let r = await postSamples('st', [
    { sampleId: 'a', ts: '2026-01-01T00:00:00Z', value: 1 },
    { sampleId: 'b', ts: '2026-01-01T00:00:01Z', value: 2 },
  ]);
  assert.equal(r.status, 200);
  let j = await r.json();
  assert.equal(j.accepted, 2);

  // 批内重复
  r = await postSamples('st', [
    { sampleId: 'x', ts: '2026-01-02T00:00:00Z', value: 9 },
    { sampleId: 'x', ts: '2026-01-02T00:00:01Z', value: 9 },
  ]);
  assert.equal(r.status, 409);
  j = await r.json();
  assert.equal(j.error.code, 'duplicate_in_batch');

  // 与已有冲突：c 不得被部分写入
  r = await postSamples('st', [
    { sampleId: 'c', ts: '2026-01-02T00:00:00Z', value: 3 },
    { sampleId: 'a', ts: '2026-01-02T00:00:01Z', value: 4 },
  ]);
  assert.equal(r.status, 409);
  j = await r.json();
  assert.equal(j.error.code, 'conflict_existing');

  // 批量大小非法
  r = await postSamples('st', []);
  assert.equal(r.status, 400);
  r = await postSamples('st', Array.from({ length: 101 }, (_, i) =>
    ({ sampleId: `z${i}`, ts: '2026-03-01T00:00:00Z', value: i })));
  assert.equal(r.status, 400);

  // 坏 JSON
  r = await api('/api/streams/st/samples', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: '{bad json',
  });
  assert.equal(r.status, 400);
});

test('GET 快照分页：遍历期间插入更早时刻数据，结果不漏不重不漂移', async () => {
  const stream = 'snap';
  // 初始 5 条，乱序写入
  const init = [
    { sampleId: 's3', ts: '2026-05-01T00:00:03Z', value: 3 },
    { sampleId: 's1', ts: '2026-05-01T00:00:01Z', value: 1 },
    { sampleId: 's2', ts: '2026-05-01T00:00:02Z', value: 2 },
    { sampleId: 's4', ts: '2026-05-01T00:00:04Z', value: 4 },
    { sampleId: 's5', ts: '2026-05-01T00:00:05Z', value: 5 },
  ];
  const pr = await postSamples(stream, init);
  assert.equal(pr.status, 200);
  const posted = await pr.json();

  // 首页 pageSize=2，固定快照
  let r = await api(`/api/streams/${stream}/samples?pageSize=2`);
  assert.equal(r.status, 200);
  let page = await r.json();
  assert.equal(page.snapshotSeq, posted.lastSeq, '快照序号不变');
  assert.deepEqual(page.items.map((i) => i.sampleId), ['s1', 's2']);
  assert.ok(page.nextCursor);
  assert.equal(page.done, false);
  const cursor1 = page.nextCursor;

  // 分页“中途”：插入 3 条更早时刻 + 1 条更晚时刻
  const ir = await postSamples(stream, [
    { sampleId: 'e1', ts: '2026-04-01T00:00:00Z', value: 100 },
    { sampleId: 'e2', ts: '2026-04-02T00:00:00Z', value: 101 },
    { sampleId: 'e3', ts: '2026-05-01T00:00:01Z', value: 102 }, // 与 s1 同时刻
    { sampleId: 's6', ts: '2026-06-01T00:00:00Z', value: 6 },
  ]);
  assert.equal(ir.status, 200);

  // 继续翻页，必须仍遍历旧快照
  const seen = [...page.items.map((i) => i.sampleId)];
  let cursor = cursor1;
  let guard = 0;
  while (cursor) {
    r = await api(`/api/streams/${stream}/samples?pageSize=2&cursor=${encodeURIComponent(cursor)}`);
    assert.equal(r.status, 200);
    page = await r.json();
    assert.equal(page.snapshotSeq, posted.lastSeq, '每次响应快照序号保持不变');
    for (const it of page.items) seen.push(it.sampleId);
    cursor = page.nextCursor;
    assert.ok(++guard < 20);
  }
  assert.deepEqual(seen, ['s1', 's2', 's3', 's4', 's5'],
    '恰好返回快照中每项一次，无漏项/重复/漂移');
  assert.equal(page.done, true);

  // 新首请求开启新快照，能看到中途插入的早时刻数据
  r = await api(`/api/streams/${stream}/samples?pageSize=100`);
  page = await r.json();
  assert.ok(page.snapshotSeq > posted.lastSeq, '新会话固定新的快照上界');
  assert.deepEqual(
    page.items.map((i) => i.sampleId),
    ['e1', 'e2', 'e3', 's1', 's2', 's3', 's4', 's5', 's6'],
    '新快照包含早到数据，且同时刻按 sampleId 排序'
  );
});

test('GET 时间范围固定在会话内', async () => {
  const stream = 'range';
  await postSamples(stream, [
    { sampleId: 'a', ts: '2026-01-01T00:00:00Z', value: 1 },
    { sampleId: 'b', ts: '2026-01-02T00:00:00Z', value: 2 },
    { sampleId: 'c', ts: '2026-01-03T00:00:00Z', value: 3 },
  ]);
  let r = await api(
    `/api/streams/${stream}/samples?pageSize=1` +
    `&from=2026-01-01T00:00:00Z&to=2026-01-02T00:00:00Z`
  );
  let page = await r.json();
  assert.deepEqual(page.items.map((i) => i.sampleId), ['a']);
  const cursor = page.nextCursor;

  // 携带游标但改变时间范围 -> 明确 400
  r = await api(
    `/api/streams/${stream}/samples?pageSize=1&cursor=${encodeURIComponent(cursor)}` +
    `&from=2026-01-01T00:00:00Z&to=2026-01-03T00:00:00Z`
  );
  assert.equal(r.status, 400);
  let j = await r.json();
  assert.equal(j.error.code, 'cursor_range_mismatch');

  // 省略范围参数（沿用会话范围）正常继续
  r = await api(`/api/streams/${stream}/samples?pageSize=1&cursor=${encodeURIComponent(cursor)}`);
  assert.equal(r.status, 200);
  page = await r.json();
  assert.deepEqual(page.items.map((i) => i.sampleId), ['b']);
  assert.equal(page.nextCursor, null, '会话范围止于 1/2，不含 c');
});

test('游标安全：篡改、跨流复用', async () => {
  // 取一个合法游标
  await postSamples('secA', [{ sampleId: 'a', ts: '2026-01-01T00:00:00Z', value: 1 }]);
  let r = await api('/api/streams/secA/samples?pageSize=1');
  let page = await r.json();
  const cursor = page.nextCursor; // a 之后 -> 结束游标其实是 null；先插两条
  await postSamples('secA', [{ sampleId: 'b', ts: '2026-01-02T00:00:00Z', value: 2 }]);
  r = await api('/api/streams/secA/samples?pageSize=1');
  page = await r.json();
  const liveCursor = page.nextCursor;
  assert.ok(liveCursor);

  // 1) 篡改
  const tampered = liveCursor.slice(0, -1) +
    (liveCursor.endsWith('A') ? 'B' : 'A');
  r = await api(`/api/streams/secA/samples?cursor=${encodeURIComponent(tampered)}`);
  assert.equal(r.status, 400);
  let j = await r.json();
  assert.equal(j.error.code, 'cursor_signature_invalid');

  // 2) 跨流复用
  r = await api(`/api/streams/secB/samples?cursor=${encodeURIComponent(liveCursor)}`);
  assert.equal(r.status, 400);
  j = await r.json();
  assert.equal(j.error.code, 'cursor_stream_mismatch');

  // 3) 格式垃圾
  r = await api('/api/streams/secA/samples?cursor=not-a-cursor');
  assert.equal(r.status, 400);
  j = await r.json();
  assert.equal(j.error.code, 'invalid_cursor');
});

test('游标在服务重启后仍可继续（同密钥 + 持久化 DB）', async () => {
  const stream = 'restart';
  await postSamples(stream, Array.from({ length: 5 }, (_, i) => ({
    sampleId: `r${i}`, ts: `2026-07-01T00:00:0${i}Z`, value: i,
  })));
  let r = await api(`/api/streams/${stream}/samples?pageSize=2`);
  let page = await r.json();
  assert.deepEqual(page.items.map((i) => i.sampleId), ['r0', 'r1']);
  const cursor = page.nextCursor;
  const snapSeq = page.snapshotSeq;

  // 重启：关闭再用同一 DB 文件、同一密钥启动新实例
  await app.close();
  app = createApp({ dbPath: join(dir, 'int.db'), cursorSecret: SECRET });
  baseUrl = await listen(app);

  // 重启前的数据仍在
  r = await fetch(baseUrl + '/health');
  assert.equal(r.status, 200);

  // 旧游标继续生效
  r = await fetch(baseUrl +
    `/api/streams/${stream}/samples?pageSize=2&cursor=${encodeURIComponent(cursor)}`);
  assert.equal(r.status, 200);
  page = await r.json();
  assert.deepEqual(page.items.map((i) => i.sampleId), ['r2', 'r3']);
  assert.equal(page.snapshotSeq, snapSeq, '快照序号跨重启不变');

  // 翻到结束
  r = await fetch(baseUrl +
    `/api/streams/${stream}/samples?pageSize=2&cursor=${encodeURIComponent(page.nextCursor)}`);
  page = await r.json();
  assert.deepEqual(page.items.map((i) => i.sampleId), ['r4']);
  assert.equal(page.nextCursor, null);
  assert.equal(page.done, true);
});

/* ============================ 并行导出 ============================ */

test('POST exports：参数校验与返回结构', async () => {
  // workerCount 越界/类型错
  for (const body of ['1', '9', '0', 'null', '"4"', '2.5']) {
    const r = await createExport('exp-val', { workerCount: JSON.parse(body) });
    assert.equal(r.status, 400, `body=${body}`);
  }
  // 坏 JSON
  let r = await api('/api/streams/exp-val/exports', {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: '{oops',
  });
  assert.equal(r.status, 400);

  // 时间范围非法
  r = await createExport('exp-val', {
    workerCount: 2, from: '2026-02-01T00:00:00Z', to: '2026-01-01T00:00:00Z',
  });
  assert.equal(r.status, 400);

  // 合法：返回不透明 token、快照序号与实际 worker 数
  r = await createExport('exp-val', {
    from: '2026-01-01T00:00:00Z', to: '2026-12-31T23:59:59Z', workerCount: 4,
  });
  assert.equal(r.status, 201);
  const j = await r.json();
  assert.equal(typeof j.exportToken, 'string');
  assert.ok(j.exportToken.includes('.'));
  assert.equal(j.workerCount, 4);
  assert.equal(typeof j.snapshotSeq, 'number');
});

test('并行导出：多 worker 并发翻页不重不漏，新增数据不进入，空分片结束', async () => {
  const stream = 'exp-parallel';
  const samples = Array.from({ length: 50 }, (_, i) => ({
    sampleId: `e${String(i).padStart(2, '0')}`,
    ts: `2026-03-0${(i % 9) + 1}T00:00:${String(i % 13).padStart(2, '0')}Z`,
    value: i,
  }));
  for (let i = 0; i < samples.length; i += 40) {
    const rr = await postSamples(stream, samples.slice(i, i + 40));
    assert.equal(rr.status, 200);
  }

  const cr = await createExport(stream, { workerCount: 6 });
  assert.equal(cr.status, 201);
  const created = await cr.json();
  const { exportToken: token, snapshotSeq } = created;
  assert.equal(created.workerCount, 6);

  // 导出创建后写入新数据（含更早时刻与更晚时刻），不得进入本次导出
  const nr = await postSamples(stream, [
    { sampleId: 'future', ts: '2030-01-01T00:00:00Z', value: 1 },
    { sampleId: 'before', ts: '2020-01-01T00:00:00Z', value: 2 },
  ]);
  assert.equal(nr.status, 200);

  const shards = await drainExport(stream, token, 6, { pageSize: 3 });
  const flat = shards.flat();
  assert.equal(flat.length, 50, '合并总数 = 创建时范围内全部观测');
  assert.equal(new Set(flat).size, 50, '无重叠/重复');
  assert.ok(!flat.includes('future') && !flat.includes('before'),
    '创建后新增数据不进入');
  assert.ok(shards.some((s) => s.length > 0), '至少一个分片非空');

  // 每页回应回带分片元数据
  const probe = await api(
    `/api/streams/${stream}/samples?exportToken=${encodeURIComponent(token)}` +
    `&workerIndex=0&pageSize=2`);
  const pj = await probe.json();
  assert.equal(pj.workerCount, 6);
  assert.equal(pj.workerIndex, 0);
  assert.equal(pj.snapshotSeq, snapshotSeq);

  // 空分片（空流导出）首页即 done
  const er = await createExport('exp-empty', { workerCount: 2 });
  const ej = await er.json();
  for (const w of [0, 1]) {
    const rr = await api(
      `/api/streams/exp-empty/samples?exportToken=${encodeURIComponent(ej.exportToken)}` +
      `&workerIndex=${w}`);
    const pg = await rr.json();
    assert.equal(rr.status, 200);
    assert.equal(pg.done, true);
    assert.equal(pg.nextCursor, null);
    assert.equal(pg.items.length, 0);
  }
});

test('并行导出：各 workerCount 下合并结果都等于普通全量快照', async () => {
  const stream = 'exp-equivalence';
  const samples = Array.from({ length: 40 }, (_, i) => ({
    sampleId: `q${String(i).padStart(2, '0')}`,
    ts: `2026-08-01T00:${String(i % 11).padStart(2, '0')}:00Z`,
    value: i,
  }));
  await postSamples(stream, samples.slice(0, 20));
  await postSamples(stream, samples.slice(20));

  // 普通快照作为基准
  const full = await api(`/api/streams/${stream}/samples?pageSize=1000`);
  const fullPage = await full.json();
  const want = fullPage.items.map((i) => i.sampleId).sort();

  for (const w of [2, 3, 4, 5, 7, 8]) {
    const cr = await createExport(stream, { workerCount: w });
    const { exportToken: token } = await cr.json();
    const shards = await drainExport(stream, token, w, { pageSize: 5 });
    const got = shards.flat().sort();
    assert.equal(got.length, 40, `w=${w} 总数`);
    assert.equal(new Set(shards.flat()).size, 40, `w=${w} 无重复`);
    assert.deepEqual(got, want, `w=${w} 合并后等于全量快照`);
  }
});

test('并行导出：时间范围固定在令牌上', async () => {
  const stream = 'exp-range';
  await postSamples(stream, [
    { sampleId: 'a', ts: '2026-01-01T00:00:00Z', value: 1 },
    { sampleId: 'b', ts: '2026-01-02T00:00:00Z', value: 2 },
    { sampleId: 'c', ts: '2026-01-03T00:00:00Z', value: 3 },
  ]);
  const cr = await createExport(stream, {
    from: '2026-01-01T00:00:00Z', to: '2026-01-02T00:00:00Z', workerCount: 2,
  });
  const { exportToken: token } = await cr.json();
  const shards = await drainExport(stream, token, 2, { pageSize: 1 });
  const got = shards.flat().sort();
  assert.deepEqual(got, ['a', 'b'], '仅范围内观测');
});

test('并行导出：令牌/游标四类误用返回可区分错误且不泄露数据', async () => {
  const stream = 'exp-errors';
  await postSamples(stream, Array.from({ length: 8 }, (_, i) => ({
    sampleId: `x${i}`, ts: `2026-09-0${i + 1}T00:00:00Z`, value: i,
  })));
  const cr = await createExport(stream, { workerCount: 3 });
  const created = await cr.json();
  const T = encodeURIComponent(created.exportToken);

  async function expectErr(name, path, code, status = 400) {
    const rr = await api(path);
    const jj = await rr.json();
    assert.equal(rr.status, status, `${name}: status`);
    assert.equal(jj.error.code, code, `${name}: code (got ${jj.error.code})`);
    assert.equal(jj.items, undefined, `${name}: 不返回数据`);
  }

  // 1) 令牌篡改
  const bad = created.exportToken;
  const mid = Math.floor(bad.length / 2);
  const tampered = bad.slice(0, mid) + (bad[mid] === 'A' ? 'B' : 'A') + bad.slice(mid + 1);
  await expectErr('篡改令牌',
    `/api/streams/${stream}/samples?exportToken=${encodeURIComponent(tampered)}&workerIndex=0`,
    'export_token_signature_invalid');

  // 2) 跨流使用
  await expectErr('跨流使用',
    `/api/streams/exp-other/samples?exportToken=${T}&workerIndex=0`,
    'export_stream_mismatch');

  // 3) workerIndex 越界 / 缺省 / 非整数
  await expectErr('workerIndex 越界',
    `/api/streams/${stream}/samples?exportToken=${T}&workerIndex=3`,
    'worker_index_out_of_range');
  await expectErr('workerIndex 缺省',
    `/api/streams/${stream}/samples?exportToken=${T}`,
    'worker_index_required');
  await expectErr('workerIndex 非数字',
    `/api/streams/${stream}/samples?exportToken=${T}&workerIndex=x`,
    'invalid_query');

  // 4) 分片游标用于另一分片
  const p0 = await api(
    `/api/streams/${stream}/samples?exportToken=${T}&workerIndex=0&pageSize=1`);
  const p0j = await p0.json();
  assert.ok(p0j.nextCursor, '前置：worker0 有续页游标');
  await expectErr('游标用于另一分片',
    `/api/streams/${stream}/samples?exportToken=${T}&workerIndex=1&pageSize=1` +
    `&cursor=${encodeURIComponent(p0j.nextCursor)}`,
    'shard_cursor_mismatch');

  // 5) 分片游标篡改
  const sc = p0j.nextCursor;
  const smid = Math.floor(sc.length / 2);
  const sTampered = sc.slice(0, smid) + (sc[smid] === 'A' ? 'B' : 'A') + sc.slice(smid + 1);
  await expectErr('分片游标篡改',
    `/api/streams/${stream}/samples?exportToken=${T}&workerIndex=0` +
    `&cursor=${encodeURIComponent(sTampered)}`,
    'shard_cursor_signature_invalid');

  // 6) 分片游标跨流（令牌先被跨流拒绝，且不返回数据）
  await expectErr('分片游标跨流',
    `/api/streams/exp-other/samples?exportToken=${T}&workerIndex=0` +
    `&cursor=${encodeURIComponent(sc)}`,
    'export_stream_mismatch');

  // 7) 分片游标不能脱离令牌走普通分页
  await expectErr('分片游标走普通通道',
    `/api/streams/${stream}/samples?cursor=${encodeURIComponent(sc)}`,
    'export_token_required');

  // 8) 普通 v1 游标不能塞进导出 cursor
  const vp = await api(`/api/streams/${stream}/samples?pageSize=1`);
  const vCursor = (await vp.json()).nextCursor;
  await expectErr('普通游标用于导出',
    `/api/streams/${stream}/samples?exportToken=${T}&workerIndex=0` +
    `&cursor=${encodeURIComponent(vCursor)}`,
    'invalid_shard_cursor');
});

test('并行导出：合法令牌与游标在服务重启后仍能完成原快照', async () => {
  const stream = 'exp-restart';
  await postSamples(stream, Array.from({ length: 21 }, (_, i) => ({
    sampleId: `z${String(i).padStart(2, '0')}`,
    ts: `2026-04-01T00:${String(i % 7).padStart(2, '0')}:00Z`,
    value: i,
  })));

  // 创建导出，取走每个分片第一页游标（记录是否已结束）
  const cr = await createExport(stream, { workerCount: 4 });
  const created = await cr.json();
  const token = created.exportToken;
  const held = [];
  const beforeRestart = [];
  for (let w = 0; w < 4; w++) {
    const rr = await api(
      `/api/streams/${stream}/samples?exportToken=${encodeURIComponent(token)}` +
      `&workerIndex=${w}&pageSize=2`);
    const pg = await rr.json();
    held[w] = { cursor: pg.nextCursor, done: pg.done };
    beforeRestart.push(...pg.items.map((i) => i.sampleId));
  }

  // 重启期间继续写入新数据
  await postSamples(stream, [
    { sampleId: 'after-restart', ts: '2019-01-01T00:00:00Z', value: 0 },
  ]);

  // 重启
  await app.close();
  app = createApp({ dbPath: join(dir, 'int.db'), cursorSecret: SECRET });
  baseUrl = await listen(app);

  // 用原令牌 + 各分片持有的游标继续未完成的分片，直到结束
  const afterItems = [];
  for (let w = 0; w < 4; w++) {
    if (held[w].done) continue; // 重启前已结束，不重复拉取
    let cursor = held[w].cursor;
    for (;;) {
      const u = `/api/streams/${stream}/samples?exportToken=${encodeURIComponent(token)}` +
        `&workerIndex=${w}&pageSize=2` +
        (cursor ? `&cursor=${encodeURIComponent(cursor)}` : '');
      const rr = await fetch(baseUrl + u);
      assert.equal(rr.status, 200, `重启后 worker ${w} 继续`);
      const pg = await rr.json();
      assert.equal(pg.snapshotSeq, created.snapshotSeq, '快照序号重启后不变');
      afterItems.push(...pg.items.map((i) => i.sampleId));
      cursor = pg.nextCursor;
      if (!cursor) break;
    }
  }
  const merged = [...beforeRestart, ...afterItems];
  assert.equal(merged.length, 21, '重启后合并仍恰好覆盖原快照 21 条');
  assert.equal(new Set(merged).size, 21, '无重复');
  assert.ok(!merged.includes('after-restart'), '重启期间新增数据不进入');
});
