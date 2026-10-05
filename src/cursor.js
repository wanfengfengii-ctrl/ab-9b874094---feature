import { createHmac, timingSafeEqual } from 'node:crypto';

/**
 * 不透明签名串（分页游标 / 导出令牌 / 分片游标共用线格式）。
 *
 * 线格式：<payloadB64>.<sigB64>，签名为 HMAC-SHA256(secret, payloadB64)。
 * 载荷版本：
 *   v1 普通快照分页游标
 *     { v:1, s:streamId, q:snapshotSeq, f:fromTs, t:toTs,
 *       p:pageSize, a:{ts, sampleId}|null }
 *   v2 并行导出（k 区分类别）
 *     导出令牌 k:'e'
 *       { v:2, k:'e', id, s, q, w, f, t }
 *     分片游标 k:'x'
 *       { v:2, k:'x', id, s, q, w, i:workerIndex, p:pageSize,
 *         a:{b:bucket, ts, sid:sampleId}|null, f, t }
 *
 * 安全性质：
 *  - 篡改任一字段 -> 签名校验失败 -> 400 *_signature_invalid
 *  - 跨流复用、workerIndex 越界、把 A 分片游标用于 B 分片 -> 应用层比对 -> 明确的 4xx
 *  - 不透明：客户端无法构造，只能原样回传服务端给出的串；错误信息不回显载荷内容
 */
export class CursorError extends Error {
  constructor(code, message, statusCode = 400) {
    super(message);
    this.name = 'CursorError';
    this.code = code;
    this.statusCode = statusCode;
  }
}

function b64urlEncode(buf) {
  return Buffer.from(buf).toString('base64url');
}

function issueSigned(secret, payload) {
  const body = b64urlEncode(JSON.stringify(payload));
  const sig = createHmac('sha256', secret).update(body).digest('base64url');
  return `${body}.${sig}`;
}

function parseSigned(secret, token, { invalidCode, invalidMessage, signatureCode, maxLength = 4096 }) {
  if (typeof token !== 'string' || token.length === 0 || token.length > maxLength) {
    throw new CursorError(invalidCode, invalidMessage);
  }
  const dot = token.lastIndexOf('.');
  if (dot <= 0) {
    throw new CursorError(invalidCode, invalidMessage);
  }
  const body = token.slice(0, dot);
  const sig = token.slice(dot + 1);

  const expected = createHmac('sha256', secret).update(body).digest();
  let given;
  try {
    given = Buffer.from(sig, 'base64url');
  } catch {
    throw new CursorError(invalidCode, invalidMessage);
  }
  if (given.length !== expected.length || !timingSafeEqual(given, expected)) {
    throw new CursorError(
      signatureCode,
      '签名无效（令牌/游标可能已被篡改）'
    );
  }

  let payload;
  try {
    payload = JSON.parse(Buffer.from(body, 'base64url').toString('utf8'));
  } catch {
    throw new CursorError(invalidCode, invalidMessage);
  }
  return payload;
}

const isTs = (x) => typeof x === 'string' && x.length > 0;
const isPosInt = (x) => Number.isInteger(x) && x >= 0;

/* ============================ v1 普通分页游标 ============================ */

export function issueCursor(secret, payload) {
  return issueSigned(secret, payload);
}

export function parseCursor(secret, token) {
  const payload = parseSigned(secret, token, {
    invalidCode: 'invalid_cursor',
    invalidMessage: '游标格式非法',
    signatureCode: 'cursor_signature_invalid',
  });
  // v2 分片游标不能走普通快照分页通道
  if (payload && payload.v === 2) {
    throw new CursorError(
      'export_token_required',
      '该游标属于并行导出任务，请求必须携带匹配的 exportToken 与 workerIndex'
    );
  }
  const ok =
    payload &&
    payload.v === 1 &&
    typeof payload.s === 'string' &&
    isPosInt(payload.q) &&
    isTs(payload.f) &&
    isTs(payload.t) &&
    Number.isInteger(payload.p) && payload.p >= 1 &&
    (payload.a === null ||
      (typeof payload.a === 'object' &&
       typeof payload.a.ts === 'string' &&
       typeof payload.a.i === 'string'));
  if (!ok) {
    throw new CursorError('invalid_cursor', '游标载荷非法');
  }
  return payload;
}

/**
 * 校验游标是否可用于当前请求（流、时间范围必须与发起会话时一致）。
 * 返回规范化后的 { snapshotSeq, fromTs, toTs, pageSize, after }。
 */
export function bindCursor(payload, { streamId, fromTs, toTs, pageSize }) {
  if (payload.s !== streamId) {
    throw new CursorError(
      'cursor_stream_mismatch',
      '游标属于其他数据流，不能跨流复用'
    );
  }
  if (fromTs != null && fromTs !== payload.f) {
    throw new CursorError(
      'cursor_range_mismatch',
      '时间范围起点已改变，请用原始范围或开启新快照会话'
    );
  }
  if (toTs != null && toTs !== payload.t) {
    throw new CursorError(
      'cursor_range_mismatch',
      '时间范围终点已改变，请用原始范围或开启新快照会话'
    );
  }
  return {
    snapshotSeq: payload.q,
    fromTs: payload.f,
    toTs: payload.t,
    pageSize: pageSize ?? payload.p,
    after: payload.a ? { ts: payload.a.ts, sampleId: payload.a.i } : null,
  };
}

/* ============================ v2 并行导出令牌 ============================ */

/**
 * 签发不透明导出令牌。
 * @param {string} secret
 * @param {{id:string, streamId:string, snapshotSeq:number,
 *          workerCount:number, fromTs:string, toTs:string}} e
 */
export function issueExportToken(secret, e) {
  return issueSigned(secret, {
    v: 2, k: 'e',
    id: e.id, s: e.streamId, q: e.snapshotSeq,
    w: e.workerCount, f: e.fromTs, t: e.toTs,
  });
}

export function parseExportToken(secret, token) {
  const p = parseSigned(secret, token, {
    invalidCode: 'invalid_export_token',
    invalidMessage: 'exportToken 格式非法',
    signatureCode: 'export_token_signature_invalid',
  });
  const ok =
    p && p.v === 2 && p.k === 'e' &&
    typeof p.id === 'string' && p.id.length > 0 &&
    typeof p.s === 'string' &&
    isPosInt(p.q) &&
    Number.isInteger(p.w) && p.w >= 2 && p.w <= 8 &&
    isTs(p.f) && isTs(p.t);
  if (!ok) {
    throw new CursorError('invalid_export_token', 'exportToken 载荷非法');
  }
  return {
    id: p.id, streamId: p.s, snapshotSeq: p.q,
    workerCount: p.w, fromTs: p.f, toTs: p.t,
  };
}

/** 令牌只能在其声明的流上使用；其余字段（快照/范围/worker 数）以令牌为准。 */
export function bindExportToken(token, { streamId }) {
  if (token.streamId !== streamId) {
    throw new CursorError(
      'export_stream_mismatch',
      'exportToken 属于其他数据流，不能跨流使用'
    );
  }
  return token;
}

/* ============================ v2 分片游标 ============================ */

/**
 * 签发某导出任务某 worker 的续页游标。
 * @param {string} secret
 * @param {{id:string, streamId:string, snapshotSeq:number, workerCount:number,
 *          workerIndex:number, fromTs:string, toTs:string, pageSize:number,
 *          after:{bucket:number, ts:string, sampleId:string}|null}} x
 */
export function issueShardCursor(secret, x) {
  return issueSigned(secret, {
    v: 2, k: 'x',
    id: x.id, s: x.streamId, q: x.snapshotSeq, w: x.workerCount,
    i: x.workerIndex, f: x.fromTs, t: x.toTs, p: x.pageSize,
    a: x.after
      ? { b: x.after.bucket, ts: x.after.ts, sid: x.after.sampleId }
      : null,
  });
}

export function parseShardCursor(secret, token) {
  const p = parseSigned(secret, token, {
    invalidCode: 'invalid_shard_cursor',
    invalidMessage: '分片游标格式非法',
    signatureCode: 'shard_cursor_signature_invalid',
  });
  const ok =
    p && p.v === 2 && p.k === 'x' &&
    typeof p.id === 'string' && p.id.length > 0 &&
    typeof p.s === 'string' &&
    isPosInt(p.q) &&
    Number.isInteger(p.w) && p.w >= 2 && p.w <= 8 &&
    Number.isInteger(p.i) && p.i >= 0 && p.i < p.w &&
    isTs(p.f) && isTs(p.t) &&
    Number.isInteger(p.p) && p.p >= 1 &&
    (p.a === null ||
      (typeof p.a === 'object' &&
       Number.isInteger(p.a.b) && p.a.b >= 0 && p.a.b < 8 &&
       typeof p.a.ts === 'string' &&
       typeof p.a.sid === 'string'));
  if (!ok) {
    throw new CursorError('invalid_shard_cursor', '分片游标载荷非法');
  }
  return {
    id: p.id, streamId: p.s, snapshotSeq: p.q, workerCount: p.w,
    workerIndex: p.i, fromTs: p.f, toTs: p.t, pageSize: p.p,
    after: p.a ? { bucket: p.a.b, ts: p.a.ts, sampleId: p.a.sid } : null,
  };
}

/**
 * 校验分片游标与当前（令牌 + 请求路径 + workerIndex）严格匹配。
 * 游标只能用于签发它的那个导出任务的那个分片；换 workerIndex、换导出、
 * 换流、换时间范围（范围由令牌固定，无法通过查询参数改变）一律拒绝。
 */
export function bindShardCursor(cur, { token, streamId, workerIndex, pageSize }) {
  if (cur.streamId !== streamId) {
    throw new CursorError(
      'shard_stream_mismatch',
      '分片游标属于其他数据流，不能跨流使用'
    );
  }
  if (cur.id !== token.id || cur.snapshotSeq !== token.snapshotSeq ||
      cur.workerCount !== token.workerCount ||
      cur.fromTs !== token.fromTs || cur.toTs !== token.toTs) {
    throw new CursorError(
      'shard_cursor_mismatch',
      '分片游标与当前 exportToken 不属于同一次导出'
    );
  }
  if (cur.workerIndex !== workerIndex) {
    throw new CursorError(
      'shard_cursor_mismatch',
      '该游标属于另一个工作分片，不能在当前 workerIndex 上使用'
    );
  }
  return {
    snapshotSeq: cur.snapshotSeq,
    fromTs: cur.fromTs,
    toTs: cur.toTs,
    workerCount: cur.workerCount,
    workerIndex: cur.workerIndex,
    pageSize: pageSize ?? cur.pageSize,
    after: cur.after,
  };
}
