import { createServer } from 'node:http';
import { randomBytes } from 'node:crypto';
import { Store, BatchConflictError } from './db.js';
import {
  issueCursor,
  parseCursor,
  bindCursor,
  bindExportToken,
  bindShardCursor,
  CursorError,
} from './cursor.js';
import {
  validateBatch,
  validateQuery,
  validateExportCreate,
  ValidationError,
} from './validation.js';

const STREAM_RE = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,199}$/;
const DEFAULT_EXPORT_WORKERS = 4;

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

  async function readJson(req, { allowEmpty = false } = {}) {
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
      if (allowEmpty) return {};
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
   *  - 普通快照（无 exportToken）：首次请求固定当前接收序号为快照上界；
   *    后续请求（带 cursor）沿用发起时的快照序号与时间范围，重启后仍可用。
   *  - 并行导出分片（带 exportToken + workerIndex）：沿导出创建时固定的同一份
   *    快照翻本分片页；游标绑定导出令牌与分片编号，串用即明确报错。
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

    if (q.exportToken !== null) {
      return handleShardList(res, streamId, q, url);
    }

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

    return sendSnapshotPage(res, {
      kind: 'snapshot',
      streamId, snapshotSeq, fromTs, toTs, pageSize, after,
    });
  }

  /** 并行导出：读取 exportToken 指定会话的第 workerIndex 个分片页。 */
  function handleShardList(res, streamId, q, url) {
    let payload;
    try {
      payload = parseCursor(cursorSecret, q.exportToken);
    } catch (err) {
      if (err instanceof CursorError) {
        // 令牌与游标同一线格式，但语义是导出令牌：归一到导出令牌错误码
        const code =
          err.code === 'cursor_signature_invalid' ? 'export_token_invalid'
          : err.code === 'invalid_cursor' ? 'invalid_export_token'
          : err.code;
        return sendError(res, err.statusCode, code, err.message);
      }
      throw err;
    }

    let exportRec;
    try {
      exportRec = bindExportToken(payload, { streamId }, (id) => store.getExport(id));
    } catch (err) {
      if (err instanceof CursorError) {
        return sendError(res, err.statusCode, err.code, err.message);
      }
      throw err;
    }

    const tokenId = payload.d;

    // 时间范围在创建导出时即固定：携带令牌又显式给出不同 from/to -> 明确报错。
    if (url.searchParams.has('from') && q.fromTs !== exportRec.fromTs) {
      return sendError(res, 400, 'export_range_mismatch',
        `时间范围起点与导出创建时不一致（创建时为 ${exportRec.fromTs}）`);
    }
    if (url.searchParams.has('to') && q.toTs !== exportRec.toTs) {
      return sendError(res, 400, 'export_range_mismatch',
        `时间范围终点与导出创建时不一致（创建时为 ${exportRec.toTs}）`);
    }

    if (q.workerIndex >= exportRec.workerCount) {
      return sendError(
        res, 400, 'worker_index_out_of_range',
        `workerIndex=${q.workerIndex} 越界：该导出共有 ${exportRec.workerCount} 个工作进程（编号 0..${exportRec.workerCount - 1}）`,
        { workerIndex: q.workerIndex, workerCount: exportRec.workerCount }
      );
    }

    const workerIndex = q.workerIndex;
    let snapshotSeq, fromTs, toTs, pageSize, after;
    if (q.cursor) {
      let cursorPayload;
      try {
        cursorPayload = parseCursor(cursorSecret, q.cursor);
      } catch (err) {
        if (err instanceof CursorError) {
          return sendError(res, err.statusCode, err.code, err.message);
        }
        throw err;
      }
      try {
        ({ snapshotSeq, fromTs, toTs, pageSize, after } = bindShardCursor(
          cursorPayload,
          {
            exportRec,
            tokenId,
            streamId,
            workerIndex,
            pageSize: url.searchParams.has('pageSize') ? q.pageSize : null,
          }
        ));
      } catch (err) {
        if (err instanceof CursorError) {
          return sendError(res, err.statusCode, err.code, err.message);
        }
        throw err;
      }
    } else {
      ({ snapshotSeq, fromTs, toTs } = exportRec);
      pageSize = q.pageSize;
      after = null;
    }

    return sendSnapshotPage(res, {
      kind: 'shard',
      streamId, snapshotSeq, fromTs, toTs, pageSize, after,
      workerCount: exportRec.workerCount,
      workerIndex,
      tokenId,
    });
  }

  /**
   * 读取一页并输出。kind='snapshot' 走普通快照；kind='shard' 走导出分片。
   * 下一页游标（如有）绑定对应会话，分片游标额外绑定 tokenId 与 workerIndex。
   */
  function sendSnapshotPage(res, ctx) {
    const { streamId, snapshotSeq, fromTs, toTs, pageSize, after } = ctx;
    const limit = pageSize + 1; // 多取一条以判断是否还有下一页
    const rows =
      ctx.kind === 'shard'
        ? store.readShardPage(
            {
              streamId,
              snapshotSeq,
              fromTs,
              toTs,
              workerCount: ctx.workerCount,
              workerIndex: ctx.workerIndex,
            },
            after,
            limit
          )
        : store.readPage(streamId, snapshotSeq, { fromTs, toTs }, after, limit);
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
      const cursorPayload = {
        v: 1,
        s: streamId,
        q: snapshotSeq,
        f: fromTs,
        t: toTs,
        p: pageSize,
        a: { ts: last.ts, i: last.sampleId },
      };
      if (ctx.kind === 'shard') {
        cursorPayload.e = ctx.tokenId;
        cursorPayload.w = ctx.workerIndex;
      }
      nextCursor = issueCursor(cursorSecret, cursorPayload);
    }

    return send(res, 200, {
      items,
      pageSize,
      snapshotSeq,
      ...(ctx.kind === 'shard'
        ? {
            exportToken: issueCursor(cursorSecret, { v: 2, d: ctx.tokenId }),
            workerIndex: ctx.workerIndex,
            workerCount: ctx.workerCount,
          }
        : {}),
      nextCursor,
      done: nextCursor === null,
    });
  }

  /**
   * POST /api/streams/:streamId/exports
   * 创建并行导出：固定当前接收序号为快照上界（创建后到达的数据一律不进入），
   * 返回不透明 exportToken、snapshotSeq 与实际工作进程数。
   */
  async function handleCreateExport(req, res, streamId) {
    const body = await readJson(req, { allowEmpty: true });
    let params;
    try {
      params = validateExportCreate(body);
    } catch (err) {
      if (err instanceof ValidationError) {
        return sendError(res, 400, 'invalid_request', err.message, err.details);
      }
      throw err;
    }
    const workerCount = params.workerCount ?? DEFAULT_EXPORT_WORKERS;
    const snapshotSeq = store.currentSeq();
    const tokenId = randomBytes(18).toString('base64url'); // 不随响应暴露原文
    store.createExport({
      tokenId,
      streamId,
      snapshotSeq,
      fromTs: params.fromTs,
      toTs: params.toTs,
      workerCount,
      createdAt: new Date().toISOString(),
    });
    return send(res, 201, {
      exportToken: issueCursor(cursorSecret, { v: 2, d: tokenId }),
      snapshotSeq,
      workerCount,
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
      const exportsMatch = url.pathname.match(/^\/api\/streams\/([^/]+)\/exports\/?$/);
      const m = samplesMatch ?? exportsMatch;
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

      if (exportsMatch) {
        if (req.method !== 'POST') {
          return sendError(res, 405, 'method_not_allowed', '该端点仅支持 POST');
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
