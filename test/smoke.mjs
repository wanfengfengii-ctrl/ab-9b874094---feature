/**
 * API 冒烟测试。
 *
 * 用法：
 *   BASE_URL=http://api:8080 node test/smoke.mjs   # 对已运行服务冒烟（compose verify 用）
 *   node test/smoke.mjs --local                    # 本地临时拉起服务，含“重启后续游标”
 *
 * 任一检查失败 -> 进程以非零退出码结束。
 */
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import process from 'node:process';

let failures = 0;
const logs = [];
function check(name, cond, detail = '') {
  if (cond) {
    logs.push(`  ✓ ${name}`);
  } else {
    logs.push(`  ✗ ${name} ${detail}`);
    failures += 1;
  }
}

async function waitForHealth(baseUrl, timeoutMs = 15000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const r = await fetch(baseUrl + '/health');
      if (r.ok) return true;
    } catch { /* 重试 */ }
    await new Promise((r) => setTimeout(r, 200));
  }
  return false;
}

async function runSmoke(baseUrl, { restartFlow = null } = {}) {
  logs.push(`# 对 ${baseUrl} 进行 API 冒烟`);
  // 每次运行使用唯一流名：对持久卷重复执行 verify 也互不污染（清洁会话）。
  const runId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  const S = (name) => `${name}-${runId}`;
  const get = (p) => fetch(baseUrl + p);
  const post = (stream, samples) =>
    fetch(`${baseUrl}/api/streams/${stream}/samples`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ samples }),
    });
  const createExport = (stream, body = {}) =>
    fetch(`${baseUrl}/api/streams/${stream}/exports`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    });

  // 1) 健康检查
  check('GET /health 200', await waitForHealth(baseUrl));

  // 2) 正常批量接收
  const stream = S('buoy-smoke');
  let r = await post(stream, [
    { sampleId: 'b03', ts: '2026-09-01T00:00:03Z', value: 30 },
    { sampleId: 'b01', ts: '2026-09-01T00:00:01Z', value: 10 },
    { sampleId: 'b02', ts: '2026-09-01T00:00:02Z', value: 20 },
  ]);
  check('POST 合法批次 200', r.status === 200, `status=${r.status}`);
  const posted = await r.json();

  // 3) 批内重复 -> 409，整批拒绝
  r = await post(stream, [
    { sampleId: 'x', ts: '2026-09-02T00:00:00Z', value: 1 },
    { sampleId: 'x', ts: '2026-09-02T00:00:01Z', value: 2 },
  ]);
  let body = await r.json();
  check('POST 批内重复 -> 409 duplicate_in_batch',
    r.status === 409 && body.error.code === 'duplicate_in_batch',
    `status=${r.status}`);

  // 4) 与已有冲突 -> 409，且同批的合法项不留部分写入
  r = await post(stream, [
    { sampleId: 'ghost', ts: '2026-09-03T00:00:00Z', value: 9 },
    { sampleId: 'b01', ts: '2026-09-03T00:00:01Z', value: 9 },
  ]);
  body = await r.json();
  check('POST 编号冲突 -> 409 conflict_existing',
    r.status === 409 && body.error.code === 'conflict_existing', `status=${r.status}`);

  // 5) 快照分页：pageSize=2 先取首页
  r = await get(`/api/streams/${stream}/samples?pageSize=2`);
  let page = await r.json();
  const snapSeq = page.snapshotSeq;
  check('首页按 (ts, sampleId) 稳定排序',
    JSON.stringify(page.items.map((i) => i.sampleId)) === JSON.stringify(['b01', 'b02']),
    JSON.stringify(page.items));
  check('首页给出下一游标', typeof page.nextCursor === 'string' && page.items.length === 2);
  check('首页 snapshotSeq 等于写入后序号', snapSeq === posted.lastSeq,
    `${snapSeq} != ${posted.lastSeq}`);
  const cursor1 = page.nextCursor;

  // 6) 分页中途插入更早时刻（及更晚时刻）数据
  r = await post(stream, [
    { sampleId: 'old1', ts: '2026-08-01T00:00:00Z', value: 100 },
    { sampleId: 'old2', ts: '2026-08-02T00:00:00Z', value: 101 },
    { sampleId: 'b04', ts: '2026-09-01T00:00:04Z', value: 40 },
  ]);
  check('中途插入早时刻批次被接收', r.status === 200, `status=${r.status}`);

  // 7) 继续遍历旧快照：不漏、不重、不漂移，snapshotSeq 不变
  const seen = ['b01', 'b02'];
  let cursor = cursor1;
  let guard = 0;
  while (cursor) {
    r = await get(`/api/streams/${stream}/samples?pageSize=2&cursor=${encodeURIComponent(cursor)}`);
    check('翻页请求 200', r.status === 200, `status=${r.status}`);
    page = await r.json();
    check('后续页 snapshotSeq 保持不变', page.snapshotSeq === snapSeq,
      `${page.snapshotSeq} != ${snapSeq}`);
    for (const it of page.items) seen.push(it.sampleId);
    cursor = page.nextCursor;
    if (++guard > 20) { check('分页守卫（死循环保护）', false); break; }
  }
  check('旧快照恰好每项一次（无漏项/重复/漂移）',
    JSON.stringify(seen) === JSON.stringify(['b01', 'b02', 'b03']),
    JSON.stringify(seen));
  check('结束标记 done=true', page.done === true && page.nextCursor === null);
  check('ghost/x 未发生部分写入',
    !seen.includes('ghost') && !seen.includes('x'));

  // 8) 游标篡改
  const tampered = cursor1.slice(0, -1) + (cursor1.endsWith('A') ? 'B' : 'A');
  r = await get(`/api/streams/${stream}/samples?cursor=${encodeURIComponent(tampered)}`);
  body = await r.json();
  check('篡改游标 -> 400 cursor_signature_invalid',
    r.status === 400 && body.error.code === 'cursor_signature_invalid',
    `status=${r.status}`);

  // 9) 跨流复用
  r = await get(`/api/streams/other-buoy/samples?cursor=${encodeURIComponent(cursor1)}`);
  body = await r.json();
  check('跨流复用游标 -> 400 cursor_stream_mismatch',
    r.status === 400 && body.error.code === 'cursor_stream_mismatch',
    `status=${r.status}`);

  // 10) 改变原时间范围
  const rangeStream = S('ranged');
  await post(rangeStream, [
    { sampleId: 'c1', ts: '2026-01-01T00:00:00Z', value: 1 },
    { sampleId: 'c2', ts: '2026-01-01T00:00:00Z', value: 2 },
  ]);
  r = await get(`/api/streams/${rangeStream}/samples?pageSize=1` +
    '&from=2026-01-01T00:00:00Z&to=2026-01-01T00:00:00Z');
  page = await r.json();
  const rangeCursor = page.nextCursor;
  r = await get(`/api/streams/${rangeStream}/samples?pageSize=1` +
    `&cursor=${encodeURIComponent(rangeCursor)}` +
    '&from=2026-01-01T00:00:00Z&to=2026-01-02T00:00:00Z');
  body = await r.json();
  check('改变时间范围 -> 400 cursor_range_mismatch',
    r.status === 400 && body.error.code === 'cursor_range_mismatch',
    `status=${r.status}`);

  // 11) 非法请求体 / 批量越界
  r = await fetch(`${baseUrl}/api/streams/${stream}/samples`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: '{oops',
  });
  check('坏 JSON -> 400', r.status === 400, `status=${r.status}`);
  r = await post(stream, []);
  check('空批次 -> 400', r.status === 400, `status=${r.status}`);
  r = await post(stream, Array.from({ length: 101 }, (_, i) =>
    ({ sampleId: `big${i}`, ts: '2026-09-04T00:00:00Z', value: i })));
  check('101 条批次 -> 400', r.status === 400, `status=${r.status}`);

  // 12) 新快照能看到早到数据
  r = await get(`/api/streams/${stream}/samples?pageSize=100`);
  page = await r.json();
  check('新会话固定新快照上界', page.snapshotSeq > snapSeq);
  check('新快照包含中途插入的早时刻数据',
    page.items.slice(0, 2).map((i) => i.sampleId).join(',') === 'old1,old2',
    JSON.stringify(page.items.map((i) => i.sampleId)));

  // 13) 并行导出冒烟：创建 -> 多工作进程并发翻分片 -> 合并恰好覆盖快照
  const expStream = S('parallel-export');
  const EXP_N = 43;
  const expSamples = Array.from({ length: EXP_N }, (_, i) => ({
    sampleId: `o${String(i).padStart(3, '0')}`,
    ts: `2026-10-01T${String(Math.floor(i / 60)).padStart(2, '0')}:${String(i % 60).padStart(2, '0')}:00Z`,
    value: i,
  }));
  for (let i = 0; i < expSamples.length; i += 17) {
    r = await post(expStream, expSamples.slice(i, i + 17));
    check('导出种子数据接收 200', r.status === 200, `status=${r.status}`);
  }

  r = await createExport(expStream, { workerCount: 6 });
  body = await r.json();
  check('POST exports 201', r.status === 201, `status=${r.status}`);
  check('返回 exportToken / snapshotSeq / 实际 workerCount',
    typeof body.exportToken === 'string' && body.exportToken.includes('.') &&
    body.workerCount === 6 && Number.isInteger(body.snapshotSeq));
  const exportToken = body.exportToken;
  const exportSnap = body.snapshotSeq;

  // 创建导出后追加“更早/更晚时刻”数据，均不得进入任一前片
  r = await post(expStream, [
    { sampleId: 'EXP_EARLY', ts: '2000-01-01T00:00:00Z', value: -1 },
    { sampleId: 'EXP_LATE', ts: '2099-01-01T00:00:00Z', value: 999 },
  ]);
  check('导出后新增数据被接收但不进入快照', r.status === 200);

  // 6 个工作进程真并发翻页（pageSize 故意很小）
  const drainWorker = async (workerIndex) => {
    const ids = [];
    let cursor;
    let guard = 0;
    do {
      const u = `/api/streams/${expStream}/samples?exportToken=${encodeURIComponent(exportToken)}` +
        `&workerIndex=${workerIndex}&pageSize=5` + (cursor ? `&cursor=${encodeURIComponent(cursor)}` : '');
      const rr = await get(u);
      if (rr.status !== 200) {
        const e = await rr.json();
        throw new Error(`worker ${workerIndex} -> ${rr.status} ${e.error.code}`);
      }
      const pg = await rr.json();
      if (pg.snapshotSeq !== exportSnap) throw new Error('snapshotSeq 漂移');
      if (pg.workerIndex !== workerIndex || pg.workerCount !== 6) {
        throw new Error('响应分片身份不符');
      }
      for (const it of pg.items) ids.push(it.sampleId);
      cursor = pg.nextCursor;
    } while (cursor && ++guard < 50);
    return ids;
  };
  let mergedIds = null;
  try {
    const shards = await Promise.all(Array.from({ length: 6 }, (_, w) => drainWorker(w)));
    mergedIds = shards.flat().sort();
  } catch (e) {
    check('并发分片翻页全部 200', false, e.message);
  }
  const expectIds = expSamples.map((s) => s.sampleId).sort();
  check('六分片合并恰好覆盖导出时全部观测（不重不漏）',
    mergedIds && JSON.stringify(mergedIds) === JSON.stringify(expectIds),
    mergedIds ? `got ${mergedIds.length} 条` : '');
  check('导出期间新增数据不进入',
    mergedIds && !mergedIds.includes('EXP_EARLY') && !mergedIds.includes('EXP_LATE'));

  // 空分片：流仅 1 条、workerCount=8，7 个分片首请求即 done
  const sparse = S('sparse-export');
  await post(sparse, [{ sampleId: 'only', ts: '2026-10-02T00:00:00Z', value: 1 }]);
  const ex8 = await (await createExport(sparse, { workerCount: 8 })).json();
  const firstShots = await Promise.all(Array.from({ length: 8 }, async (_, w) => {
    const rr = await get(`/api/streams/${sparse}/samples?exportToken=${encodeURIComponent(ex8.exportToken)}` +
      `&workerIndex=${w}&pageSize=10`);
    return rr.json();
  }));
  check('空分片首请求直接结束',
    firstShots.filter((p) => p.items.length === 0 && p.done && p.nextCursor === null).length === 7);

  // 错误矩阵：各情形可区分且不泄露数据
  const errCase = async (u, code, status = 400) => {
    const rr = await get(u);
    const b = await rr.json();
    return rr.status === status && b.error.code === code && !b.items;
  };
  check('导出令牌格式垃圾 -> 400 invalid_export_token',
    await errCase(`/api/streams/${expStream}/samples?exportToken=garbage&workerIndex=0`,
      'invalid_export_token'));
  const tokParts = exportToken.split('.');
  const tokTampered = tokParts[0] + '.' +
    tokParts[1].slice(0, -1) + (tokParts[1].endsWith('A') ? 'B' : 'A');
  check('篡改导出令牌 -> 400 export_token_invalid',
    await errCase(`/api/streams/${expStream}/samples?exportToken=${encodeURIComponent(tokTampered)}` +
      '&workerIndex=0', 'export_token_invalid'));
  check('导出令牌跨流使用 -> 400 export_stream_mismatch',
    await errCase(`/api/streams/OTHER-STREAM/samples?exportToken=${encodeURIComponent(exportToken)}` +
      '&workerIndex=0', 'export_stream_mismatch'));
  check('工作进程越界 -> 400 worker_index_out_of_range',
    await errCase(`/api/streams/${expStream}/samples?exportToken=${encodeURIComponent(exportToken)}` +
      '&workerIndex=6', 'worker_index_out_of_range'));
  check('缺 workerIndex -> 400 invalid_query',
    await errCase(`/api/streams/${expStream}/samples?exportToken=${encodeURIComponent(exportToken)}`,
      'invalid_query'));

  // 分片游标串用：拿 worker1 的游标给 worker2 -> cursor_worker_mismatch
  const p1 = await get(`/api/streams/${expStream}/samples?exportToken=${encodeURIComponent(exportToken)}` +
    '&workerIndex=1&pageSize=2').then((x) => x.json());
  check('分片游标跨分片使用 -> 400 cursor_worker_mismatch',
    await errCase(`/api/streams/${expStream}/samples?exportToken=${encodeURIComponent(exportToken)}` +
      `&workerIndex=2&pageSize=2&cursor=${encodeURIComponent(p1.nextCursor)}`,
      'cursor_worker_mismatch'));
  // 分片游标脱离 exportToken 走普通分页 -> invalid_cursor
  check('分片游标用于无令牌普通请求 -> 400 invalid_cursor',
    await errCase(`/api/streams/${expStream}/samples?pageSize=2&cursor=${encodeURIComponent(p1.nextCursor)}`,
      'invalid_cursor'));

  // 创建导出参数校验
  for (const bad of [1, 9, 0]) {
    const rr = await createExport(expStream, { workerCount: bad });
    check(`workerCount=${bad} 创建导出 -> 400`, rr.status === 400, `status=${rr.status}`);
  }
  check('GET /exports -> 405',
    (await get(`/api/streams/${expStream}/exports`)).status === 405);

  // 13) 重启后游标继续（仅本地模式可以真的重启进程）
  if (restartFlow) {
    const persistStream = S('persist');
    await post(persistStream, Array.from({ length: 5 }, (_, i) => ({
      sampleId: `p${i}`, ts: `2026-06-01T00:00:0${i}Z`, value: i,
    })));
    r = await get(`/api/streams/${persistStream}/samples?pageSize=2`);
    page = await r.json();
    const heldCursor = page.nextCursor;
    const heldSeq = page.snapshotSeq;
    check('重启前取到游标', typeof heldCursor === 'string');

    // 并行导出：重启前 worker 0/3 各翻一页并持有分片游标
    const expPersist = S('export-persist');
    const PE = 21;
    await post(expPersist, Array.from({ length: PE }, (_, i) => ({
      sampleId: `q${String(i).padStart(2, '0')}`,
      ts: `2026-06-02T00:00:${String(i).padStart(2, '0')}Z`,
      value: i,
    })));
    const pe = await (await createExport(expPersist, { workerCount: 4 })).json();
    const heldShards = await Promise.all([0, 3].map(async (w) => {
      const pg = await get(`/api/streams/${expPersist}/samples?exportToken=${encodeURIComponent(pe.exportToken)}` +
        `&workerIndex=${w}&pageSize=2`).then((x) => x.json());
      return { w, ids: pg.items.map((i) => i.sampleId), cursor: pg.nextCursor };
    }));
    check('重启前持有分片游标', heldShards.every((h) => typeof h.cursor === 'string'));
    const peToken = pe.exportToken;
    const peSnap = pe.snapshotSeq;

    const newBase = await restartFlow();
    baseUrl = newBase; // eslint-disable-line no-param-reassign

    r = await fetch(`${newBase}/health`);
    check('重启后健康检查 200', r.status === 200, `status=${r.status}`);
    r = await fetch(`${newBase}/api/streams/${persistStream}/samples?pageSize=2` +
      `&cursor=${encodeURIComponent(heldCursor)}`);
    check('重启后旧游标可用 -> 200', r.status === 200, `status=${r.status}`);
    page = await r.json();
    check('重启后接着遍历 p2,p3',
      JSON.stringify(page.items.map((i) => i.sampleId)) === JSON.stringify(['p2', 'p3']),
      JSON.stringify(page.items));
    check('重启后快照序号不变', page.snapshotSeq === heldSeq,
      `${page.snapshotSeq} != ${heldSeq}`);
    r = await fetch(`${newBase}/api/streams/${persistStream}/samples?pageSize=2` +
      `&cursor=${encodeURIComponent(page.nextCursor)}`);
    page = await r.json();
    check('重启后翻到末页并结束',
      JSON.stringify(page.items.map((i) => i.sampleId)) === JSON.stringify(['p4']) &&
      page.done === true);

    // 重启后新增数据不得进入原导出；4 个分片（0/3 用持有游标续翻）合并恰为 21 条
    await post(expPersist, [{ sampleId: 'q-after', ts: '2000-01-01T00:00:00Z', value: 0 }]);
    const finishShard = async (w, held) => {
      const ids = held ? [...held.ids] : [];
      let cursor = held ? held.cursor : undefined;
      let guard = 0;
      do {
        const u = `/api/streams/${expPersist}/samples?exportToken=${encodeURIComponent(peToken)}` +
          `&workerIndex=${w}&pageSize=3` + (cursor ? `&cursor=${encodeURIComponent(cursor)}` : '');
        const rr = await get(u);
        if (rr.status !== 200) throw new Error(`分片 ${w} 重启后续翻 ${rr.status}`);
        const pg = await rr.json();
        if (pg.snapshotSeq !== peSnap) throw new Error('分片快照序号漂移');
        ids.push(...pg.items.map((i) => i.sampleId));
        cursor = pg.nextCursor;
      } while (cursor && ++guard < 20);
      return ids;
    };
    let peMerged = null;
    try {
      const parts = await Promise.all([0, 1, 2, 3].map((w) =>
        finishShard(w, heldShards.find((h) => h.w === w))));
      peMerged = parts.flat().sort();
    } catch (e) {
      check('重启后各分片续翻 200', false, e.message);
    }
    check('重启后并行导出合并仍恰好覆盖原快照（21 条，无新增项）',
      peMerged && peMerged.length === PE &&
      new Set(peMerged).size === PE && !peMerged.includes('q-after'),
      peMerged ? `${peMerged.length} 条` : '');
  }
}

function startLocalServer(dbPath) {
  const child = spawn(process.execPath, ['src/server.js'], {
    env: {
      ...process.env,
      PORT: '0',
      HOST: '127.0.0.1',
      DB_PATH: dbPath,
      CURSOR_SECRET: 'smoke-fixed-secret',
    },
    stdio: ['ignore', 'pipe', 'inherit'],
  });
  return new Promise((resolve, reject) => {
    child.stdout.on('data', (d) => {
      const m = String(d).match(/listening on http:\/\/[^:]+:(\d+)/);
      if (m) resolve({ child, port: Number(m[1]) });
    });
    child.on('exit', (code) => reject(new Error(`服务提前退出 code=${code}`)));
  });
}

async function main() {
  const local = process.argv.includes('--local');
  let baseUrl = process.env.BASE_URL;
  let cleanup = async () => {};

  if (local || !baseUrl) {
    const dir = mkdtempSync(join(tmpdir(), 'obs-smoke-'));
    const dbPath = join(dir, 'smoke.db');
    const secret = 'smoke-fixed-secret';
    let running = await startLocalServer(dbPath);
    baseUrl = `http://127.0.0.1:${running.port}`;

    const restartFlow = async () => {
      await new Promise((res) => {
        running.child.on('exit', res);
        running.child.kill('SIGTERM');
      });
      running = await startLocalServer(dbPath);
      return `http://127.0.0.1:${running.port}`;
    };
    cleanup = async () => {
      running.child.kill('SIGTERM');
      await new Promise((r) => setTimeout(r, 200));
      rmSync(dir, { recursive: true, force: true });
    };

    try {
      await runSmoke(baseUrl, { restartFlow });
    } finally {
      await cleanup();
    }
  } else {
    await runSmoke(baseUrl, { restartFlow: null });
  }

  console.log(logs.join('\n'));
  if (failures > 0) {
    console.error(`\n冒烟结果: 失败 ${failures} 项`);
    process.exit(1);
  }
  console.log('\n冒烟结果: 全部通过');
}

main().catch((err) => {
  console.error('冒烟执行异常:', err);
  process.exit(1);
});
