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
