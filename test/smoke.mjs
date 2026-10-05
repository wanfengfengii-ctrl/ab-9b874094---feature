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

  await runExportSmoke({ baseUrlRef: () => baseUrl, setBaseUrl: (u) => { baseUrl = u; }, get, post, S, runId, restartFlow });

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
  }
}

/**
 * 并行导出冒烟：
 *  - 创建导出（可选时间范围 + 2..8 worker）返回不透明令牌/快照序号/实际 worker 数；
 *  - 多个 worker 并发翻页，分片不重叠、合并恰好覆盖快照范围，期间新增数据不进入；
 *  - 空分片首页即结束；
 *  - 令牌篡改 / 跨流 / workerIndex 越界 / 游标用于另一分片 -> 可区分错误码；
 *  - 未携带 exportToken 的旧分页行为不受影响（本文件前述步骤已覆盖）。
 * 本地模式额外验证：合法令牌与游标在服务重启后仍可完成原快照。
 */
async function runExportSmoke(ctx) {
  const { get, post, S, restartFlow } = ctx;
  const baseUrl = () => ctx.baseUrlRef();
  const jget = (p) => fetch(baseUrl() + p);

  logs.push('# 并行导出冒烟');
  const stream = S('export');
  const N = 37;
  // 时刻大量重复（同刻多条），验证按 sampleId 打散
  const samples = Array.from({ length: N }, (_, i) => ({
    sampleId: `m${String(i).padStart(2, '0')}`,
    ts: `2026-10-01T00:00:${String(i % 7).padStart(2, '0')}Z`,
    value: i,
  }));
  let r = await post(stream, samples.slice(0, 30));
  check('导出前写入首批', r.status === 200, `status=${r.status}`);

  // 创建导出：时间范围 + workerCount=5
  r = await fetch(`${baseUrl()}/api/streams/${stream}/exports`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      from: '2026-01-01T00:00:00Z',
      to: '2027-01-01T00:00:00Z',
      workerCount: 5,
    }),
  });
  check('POST exports -> 201', r.status === 201, `status=${r.status}`);
  const created = await r.json();
  check('返回不透明 exportToken',
    typeof created.exportToken === 'string' && created.exportToken.includes('.'));
  check('返回实际 workerCount=5', created.workerCount === 5);
  check('返回 snapshotSeq', Number.isInteger(created.snapshotSeq));
  const token = created.exportToken;

  // 创建后再写 7 条（含范围外与范围内），均不得进入该快照
  r = await post(stream, [
    ...samples.slice(30),
    { sampleId: 'late-arrival', ts: '2026-10-02T00:00:00Z', value: 99 },
  ]);
  check('导出创建后写入成功', r.status === 200, `status=${r.status}`);

  // workerCount 非法
  for (const bad of [1, 9]) {
    r = await fetch(`${baseUrl()}/api/streams/${stream}/exports`, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ workerCount: bad }),
    });
    check(`workerCount=${bad} -> 400`, r.status === 400, `status=${r.status}`);
  }

  // 五个 worker 并发翻页
  async function drainWorker(w) {
    const ids = [];
    let cursor = null;
    let guard = 0;
    do {
      const u = `/api/streams/${stream}/samples?exportToken=${encodeURIComponent(token)}` +
        `&workerIndex=${w}&pageSize=3` +
        (cursor ? `&cursor=${encodeURIComponent(cursor)}` : '');
      const rr = await jget(u);
      const pg = await rr.json();
      if (!rr.ok) throw new Error(`worker ${w} ${rr.status} ${pg.error?.code}`);
      ids.push(...pg.items.map((i) => i.sampleId));
      cursor = pg.nextCursor;
      if (++guard > 100) throw new Error('分页守卫');
    } while (cursor);
    return { ids };
  }

  const shards = await Promise.all([0, 1, 2, 3, 4].map((w) => drainWorker(w)));
  const counts = shards.map((s) => s.ids.length);
  logs.push(`    各分片条数: ${counts.join(', ')}`);
  const merged = shards.flatMap((s) => s.ids);
  check('合并恰好覆盖快照内 30 条',
    merged.length === 30, `got ${merged.length}`);
  check('分片互不重叠（无重复）', new Set(merged).size === 30);
  check('合并等于创建时范围内全部观测',
    JSON.stringify([...merged].sort()) ===
      JSON.stringify(samples.slice(0, 30).map((s) => s.sampleId).sort()),
    JSON.stringify([...merged].sort()));
  check('创建后新增数据未进入', !merged.includes('late-arrival'));

  // 空分片：空流导出，每个 worker 首页即 done
  r = await fetch(`${baseUrl()}/api/streams/${S('export-empty')}/exports`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ workerCount: 3 }),
  });
  const emptyExport = await r.json();
  let allEmpty = true;
  for (let w = 0; w < 3; w++) {
    const rr = await jget(
      `/api/streams/${S('export-empty')}/samples` +
      `?exportToken=${encodeURIComponent(emptyExport.exportToken)}&workerIndex=${w}`);
    const pg = await rr.json();
    if (!(rr.ok && pg.done === true && pg.items.length === 0)) allEmpty = false;
  }
  check('空分片首页直接 done', allEmpty);

  // ---- 错误路径（错误码可区分且不返回数据）----
  async function errCode(p) {
    const rr = await jget(p);
    const jj = await rr.json();
    check('错误路径返回 400 且不携带数据',
      rr.status === 400 && jj.items === undefined, `status=${rr.status}`);
    return jj.error?.code;
  }
  const T = encodeURIComponent(token);
  const mid = Math.floor(token.length / 2);
  const tampered = token.slice(0, mid) + (token[mid] === 'A' ? 'B' : 'A') + token.slice(mid + 1);
  check('令牌篡改 -> export_token_signature_invalid',
    (await errCode(`/api/streams/${stream}/samples?exportToken=${encodeURIComponent(tampered)}&workerIndex=0`))
      === 'export_token_signature_invalid');
  check('令牌跨流 -> export_stream_mismatch',
    (await errCode(`/api/streams/${S('other')}/samples?exportToken=${T}&workerIndex=0`))
      === 'export_stream_mismatch');
  check('workerIndex 越界 -> worker_index_out_of_range',
    (await errCode(`/api/streams/${stream}/samples?exportToken=${T}&workerIndex=5`))
      === 'worker_index_out_of_range');
  check('workerIndex 缺省 -> worker_index_required',
    (await errCode(`/api/streams/${stream}/samples?exportToken=${T}`))
      === 'worker_index_required');

  // 游标用于另一分片
  let pg = await (await jget(
    `/api/streams/${stream}/samples?exportToken=${T}&workerIndex=0&pageSize=1`)).json();
  const c0 = pg.nextCursor;
  check('取到 worker0 续页游标', typeof c0 === 'string');
  check('游标用于另一分片 -> shard_cursor_mismatch',
    (await errCode(`/api/streams/${stream}/samples?exportToken=${T}&workerIndex=1&pageSize=1` +
      `&cursor=${encodeURIComponent(c0)}`)) === 'shard_cursor_mismatch');
  const cmid = Math.floor(c0.length / 2);
  const cBad = c0.slice(0, cmid) + (c0[cmid] === 'A' ? 'B' : 'A') + c0.slice(cmid + 1);
  check('分片游标篡改 -> shard_cursor_signature_invalid',
    (await errCode(`/api/streams/${stream}/samples?exportToken=${T}&workerIndex=0` +
      `&cursor=${encodeURIComponent(cBad)}`)) === 'shard_cursor_signature_invalid');
  check('分片游标不能走普通分页 -> export_token_required',
    (await errCode(`/api/streams/${stream}/samples?cursor=${encodeURIComponent(c0)}`))
      === 'export_token_required');

  // ---- 重启后继续原快照（仅本地模式）----
  if (restartFlow) {
    const persistStream = S('export-persist');
    const PN = 13;
    await post(persistStream, Array.from({ length: PN }, (_, i) => ({
      sampleId: `t${String(i).padStart(2, '0')}`,
      ts: `2026-05-01T00:00:${String(i % 5).padStart(2, '0')}Z`,
      value: i,
    })));
    let rr = await fetch(`${baseUrl()}/api/streams/${persistStream}/exports`, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ workerCount: 3 }),
    });
    const pe = await rr.json();
    const pt = pe.exportToken;

    // 每分片先取首页并持有游标
    const held = [];
    const before = [];
    for (let w = 0; w < 3; w++) {
      const hh = await drainWorkerOn(persistStream, pt, w, { onlyFirst: true, fetch: jget });
      held.push(hh);
      before.push(...hh.firstIds);
    }

    // 重启，并在重启后写入新数据
    const newBase = await restartFlow();
    ctx.setBaseUrl(newBase);
    await post(persistStream, [
      { sampleId: 'post-restart', ts: '2018-01-01T00:00:00Z', value: 0 },
    ]);

    // 用原令牌与游标完成剩余分片
    const afterIds = [];
    for (let w = 0; w < 3; w++) {
      if (held[w].done) continue;
      let cursor = held[w].cursor;
      let guard = 0;
      do {
        const u = `/api/streams/${persistStream}/samples?exportToken=${encodeURIComponent(pt)}` +
          `&workerIndex=${w}&pageSize=3` +
          (cursor ? `&cursor=${encodeURIComponent(cursor)}` : '');
        const x = await fetch(newBase + u);
        const j = await x.json();
        check('重启后分片请求 200', x.ok, `w=${w} status=${x.status}`);
        check('重启后 snapshotSeq 不变', j.snapshotSeq === pe.snapshotSeq);
        afterIds.push(...j.items.map((i) => i.sampleId));
        cursor = j.nextCursor;
        if (++guard > 50) throw new Error('守卫');
      } while (cursor);
    }
    const allIds = [...before, ...afterIds];
    check('重启后各分片合并仍恰好覆盖原快照',
      allIds.length === PN && new Set(allIds).size === PN,
      `got ${allIds.length}`);
    check('重启后新增数据不进入', !allIds.includes('post-restart'));
  }
}

/** 在指定流上取某 worker 分片的第一页（重启流程用）。 */
async function drainWorkerOn(stream, token, w, { onlyFirst, fetch }) {
  const u = `/api/streams/${stream}/samples?exportToken=${encodeURIComponent(token)}` +
    `&workerIndex=${w}&pageSize=3`;
  const rr = await fetch(u);
  const pg = await rr.json();
  if (!rr.ok) throw new Error(`w=${w} ${rr.status}`);
  return {
    firstIds: pg.items.map((i) => i.sampleId),
    cursor: pg.nextCursor,
    done: pg.done,
  };
}

function startLocalServer(dbPath) {  const child = spawn(process.execPath, ['src/server.js'], {
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
