import { createServer } from 'node:http';
import { randomBytes } from 'node:crypto';
import { Store, BatchConflictError } from './db.js';
import {
  issueCursor,
  parseCursor,
  bindCursor,
  issueExportToken,
  parseExportToken,
  bindExportToken,
  issueShardCursor,
  parseShardCursor,
  bindShardCursor,
  CursorError,
} from './cursor.js';
import {
  validateBatch,
  validateQuery,
  validateExport,
  ValidationError,
} from './validation.js';
import { bucketsForWorker } from './shard.js';

const STREAM_RE = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,199}$/;

export function createApp({ dbPath, cursorSecret = randomBytes(32).toString('hex') } = {}) {
  const store = new Store(dbPath ?? process.env.DB_PATH ?? '/data/observations.db');

  function send(res, status, payload) {
    const body = JSON.stringify(payload);
    res.writeHead(status, {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
    });
    res.end(body);
  }

  function sendError(res, status, code, message, details) {
    send(res, status, {
      error: { code, message, ...(details ? { details } : {}) },
    });
  }

  async function readJson(req) {
    const chunks = [];
    let size = 0;
    for await (const chunk of req) {
      size += chunk.length;
      if (size > 5 * 1024 * 1024) {
        throw new ValidationError('请求体过大（上限 5 MiB）');
      }
      chunks.push(chunk);
    }
    if (chunks.length === 0) {
      throw new ValidationError('缺少 JSON 请求体');
    }
    try {
      return JSON.parse(Buffer.concat(chunks).toString('utf8'));
    } catch {
      throw new ValidationError('请求体不是合法 JSON');
    }
  }

  /**
   * GET /api/streams/:streamId/samples
   *
   * 两种模式：
   *  - 无 exportToken（兼容）：首次请求固定当前接收序号为快照上界；
   *    后续请求（带 v1 cursor）沿用发起时的快照与时间范围，游标重启后仍可用。
   *  - 带 exportToken（并行导出）：令牌固定快照、范围与 worker 数；
   *    workerIndex 零基；cursor 为该分片自己的续页游标，分片间不重叠、
   *    合并恰好覆盖创建导出时范围内的全部观测。
   */
  function handleList(req, res, streamId, url) {
    let q;
    try {
      q = validateQuery(url.searchParams);
    } catch (err) {
      if (err instanceof ValidationError) {
        return sendError(res, 400, 'invalid_query', err.message, err.details);
      }
      throw err;
    }
    return q.exportToken
      ? handleShardList(res, streamId, q, url)
      : handleSnapshotList(res, streamId, q, url);
  }

  function handleSnapshotList(res, streamId, q, url) {
    let snapshotSeq, fromTs, toTs, pageSize, after;

    if (q.cursor) {
      let payload;
      try {
        payload = parseCursor(cursorSecret, q.cursor);
      } catch (err) {
        if (err instanceof CursorError) {
          return sendError(res, err.statusCode, err.code, err.message);
        }
        throw err;
      }
      try {
        ({ snapshotSeq, fromTs, toTs, pageSize, after } = bindCursor(payload, {
          streamId,
          fromTs: url.searchParams.has('from') ? q.fromTs : null,
          toTs: url.searchParams.has('to') ? q.toTs : null,
          pageSize: url.searchParams.has('pageSize') ? q.pageSize : null,
        }));
      } catch (err) {
        if (err instanceof CursorError) {
          return sendError(res, err.statusCode, err.code, err.message);
        }
        throw err;
      }
    } else {
      // 首请求：快照 = 此刻已接收的全部数据
      snapshotSeq = store.currentSeq();
      fromTs = q.fromTs;
      toTs = q.toTs;
      pageSize = q.pageSize;
      after = null;
    }

    const limit = pageSize + 1; // 多取一条以判断是否还有下一页
    const rows = store.readPage(
      streamId, snapshotSeq, { fromTs, toTs }, after, limit
    );
    const hasMore = rows.length > pageSize;
    const page = hasMore ? rows.slice(0, pageSize) : rows;

    const items = page.map((r) => ({
      sampleId: r.sampleId,
      ts: r.ts,
      value: r.value,
    }));

    let nextCursor = null;
    if (hasMore && page.length > 0) {
      const last = page[page.length - 1];
      nextCursor = issueCursor(cursorSecret, {
        v: 1,
        s: streamId,
        q: snapshotSeq,
        f: fromTs,
        t: toTs,
        p: pageSize,
        a: { ts: last.ts, i: last.sampleId },
      });
    }

    return send(res, 200, {
      items,
      pageSize,
      snapshotSeq,
      nextCursor,
      done: nextCursor === null,
    });
  }

  /** POST /api/streams/:streamId/exports —— 创建一次并行导出（固定稳定快照）。 */
  async function handleCreateExport(req, res, streamId) {
    const body = await readJson(req);
    const { fromTs, toTs, workerCount } = validateExport(body);
    const snapshotSeq = store.currentSeq();
    // 随机导出 id 仅用于把“游标用于另一次导出”与其他误用区分；不可猜测，不回显载荷
    const id = randomBytes(16).toString('base64url');
    const exportToken = issueExportToken(cursorSecret, {
      id, streamId, snapshotSeq, workerCount, fromTs, toTs,
    });
    return send(res, 201, {
      exportToken,
      snapshotSeq,
      workerCount,
    });
  }

  /** GET samples（并行导出模式）：读取某 worker 负责的分片页。 */
  function handleShardList(res, streamId, q, url) {
    let token;
    try {
      token = parseExportToken(cursorSecret, q.exportToken);
      bindExportToken(token, { streamId });
    } catch (err) {
      if (err instanceof CursorError) {
        return sendError(res, err.statusCode, err.code, err.message);
      }
      throw err;
    }

    // workerIndex 为必填（零基），先做格式/范围校验
    if (q.workerIndex === null) {
      return sendError(res, 400, 'worker_index_required',
        '携带 exportToken 时必须提供零基 workerIndex');
    }
    if (q.workerIndex >= token.workerCount) {
      return sendError(res, 400, 'worker_index_out_of_range',
        `workerIndex 越界：该导出 workerCount=${token.workerCount}，` +
        `允许 0..${token.workerCount - 1}`);
    }
    const workerIndex = q.workerIndex;

    // 时间范围与快照完全由令牌固定，忽略查询参数中可能携带的 from/to，
    // 保证任何工作进程都只能读到创建导出时锁定的那一份范围。
    let snapshotSeq = token.snapshotSeq;
    let fromTs = token.fromTs;
    let toTs = token.toTs;
    let pageSize = q.pageSize;
    let after = null;

    if (q.cursor) {
      let cur;
      try {
        cur = parseShardCursor(cursorSecret, q.cursor);
        // 页大小与普通分页一致：未显式携带 pageSize 则沿用游标中的值
        const reqPageSize = url.searchParams.has('pageSize') ? q.pageSize : null;
        ({ snapshotSeq, fromTs, toTs, pageSize, after } =
          bindShardCursor(cur, { token, streamId, workerIndex, pageSize: reqPageSize }));
      } catch (err) {
        if (err instanceof CursorError) {
          return sendError(res, err.statusCode, err.code, err.message);
        }
        throw err;
      }
    }

    const buckets = bucketsForWorker(token.workerCount, workerIndex);
    const limit = pageSize + 1;
    const rows = store.readShardPage(
      streamId, snapshotSeq, { fromTs, toTs }, buckets, after, limit
    );
    const hasMore = rows.length > pageSize;
    const page = hasMore ? rows.slice(0, pageSize) : rows;

    const items = page.map((r) => ({
      sampleId: r.sampleId,
      ts: r.ts,
      value: r.value,
    }));

    let nextCursor = null;
    if (hasMore && page.length > 0) {
      const last = page[page.length - 1];
      nextCursor = issueShardCursor(cursorSecret, {
        id: token.id,
        streamId,
        snapshotSeq,
        workerCount: token.workerCount,
        workerIndex,
        fromTs,
        toTs,
        pageSize,
        after: { bucket: last.bucket, ts: last.ts, sampleId: last.sampleId },
      });
    }

    return send(res, 200, {
      items,
      pageSize,
      snapshotSeq,
      workerIndex,
      workerCount: token.workerCount,
      nextCursor,
      done: nextCursor === null,
    });
  }

  async function handlePost(req, res, streamId) {
    const body = await readJson(req);
    const { items } = validateBatch(body);
    try {
      const result = store.insertBatch(streamId, items);
      return send(res, 200, {
        accepted: result.count,
        firstSeq: result.firstSeq,
        lastSeq: result.lastSeq,
      });
    } catch (err) {
      if (err instanceof BatchConflictError) {
        // 整批已回滚，无部分写入
        return sendError(res, 409, err.details.reason, err.message, err.details);
      }
      throw err;
    }
  }

  const handler = async (req, res) => {
    try {
      const url = new URL(req.url, 'http://localhost');
      if (req.method === 'GET' && url.pathname === '/health') {
        return send(res, 200, {
          status: 'ok',
          currentSeq: store.currentSeq(),
          totalSamples: store.totalSamples(),
        });
      }

      const samplesMatch = url.pathname.match(/^\/api\/streams\/([^/]+)\/samples\/?$/);
      const exportMatch = url.pathname.match(/^\/api\/streams\/([^/]+)\/exports\/?$/);
      const m = samplesMatch ?? exportMatch;
      if (!m) {
        return sendError(res, 404, 'not_found', `路径不存在: ${url.pathname}`);
      }
      let streamId;
      try {
        streamId = decodeURIComponent(m[1]);
      } catch {
        return sendError(res, 400, 'invalid_stream_id', 'streamId 编码非法');
      }
      if (!STREAM_RE.test(streamId)) {
        return sendError(res, 400, 'invalid_stream_id', 'streamId 非法');
      }

      if (exportMatch) {
        if (req.method !== 'POST') {
          return sendError(res, 405, 'method_not_allowed', '仅支持 POST 创建导出');
        }
        return await handleCreateExport(req, res, streamId);
      }
      if (req.method === 'POST') return await handlePost(req, res, streamId);
      if (req.method === 'GET') return handleList(req, res, streamId, url);
      return sendError(res, 405, 'method_not_allowed', '仅支持 GET / POST');
    } catch (err) {
      if (err instanceof ValidationError) {
        return sendError(res, 400, 'invalid_request', err.message, err.details);
      }
      console.error('unhandled error:', err);
      if (!res.headersSent) {
        sendError(res, 500, 'internal_error', '服务内部错误');
      } else {
        res.end();
      }
    }
  };

  const server = createServer(handler);
  return {
    store,
    server,
    close: () =>
      new Promise((resolve) =>
        server.close(() => {
          store.close();
          resolve();
        })
      ),
  };
}

export function startServer() {
  const port = Number(process.env.PORT ?? 8080);
  const host = process.env.HOST ?? '0.0.0.0';
  const secret = process.env.CURSOR_SECRET; // 缺省时随机 -> 游标重启失效，故 compose 固定注入
  const app = createApp(
    secret ? { cursorSecret: secret } : {}
  );
  app.server.listen(port, host, () => {
    const actualPort = app.server.address().port;
    console.log(`ocean observation API listening on http://${host}:${actualPort}`);
  });
  return app;
}
