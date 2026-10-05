import { createHmac, timingSafeEqual } from 'node:crypto';

/**
 * 不透明分页游标。
 *
 * 载荷（JSON，base64url）：
 *   { v:1, s:streamId, q:snapshotSeq, f:fromTs, t:toTs, p:pageSize, a:{ts, sampleId}|null }
 * 线格式：<payloadB64>.<sigB64>，签名为 HMAC-SHA256(secret, payloadB64)。
 *
 * 安全性质：
 *  - 篡改任一字段 -> 签名校验失败 -> 400 cursor_signature_invalid
 *  - 跨流复用 / 改变原时间范围 -> 由应用比对载荷 -> 明确的 4xx
 *  - 不透明：客户端无法构造，只能回传服务端给出的串
 */
export class CursorError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'CursorError';
    this.code = code;
    this.statusCode = 400;
  }
}

function b64urlEncode(buf) {
  return Buffer.from(buf).toString('base64url');
}

export function issueCursor(secret, payload) {
  const body = b64urlEncode(JSON.stringify(payload));
  const sig = createHmac('sha256', secret).update(body).digest('base64url');
  return `${body}.${sig}`;
}

export function parseCursor(secret, token) {
  if (typeof token !== 'string' || token.length === 0 || token.length > 4096) {
    throw new CursorError('invalid_cursor', '游标格式非法');
  }
  const dot = token.lastIndexOf('.');
  if (dot <= 0) {
    throw new CursorError('invalid_cursor', '游标格式非法');
  }
  const body = token.slice(0, dot);
  const sig = token.slice(dot + 1);

  const expected = createHmac('sha256', secret).update(body).digest();
  let given;
  try {
    given = Buffer.from(sig, 'base64url');
  } catch {
    throw new CursorError('invalid_cursor', '游标编码非法');
  }
  if (given.length !== expected.length || !timingSafeEqual(given, expected)) {
    throw new CursorError('cursor_signature_invalid', '游标签名无效（可能已被篡改）');
  }

  let payload;
  try {
    payload = JSON.parse(Buffer.from(body, 'base64url').toString('utf8'));
  } catch {
    throw new CursorError('invalid_cursor', '游标内容无法解析');
  }

  const ok =
    payload &&
    payload.v === 1 &&
    typeof payload.s === 'string' &&
    Number.isInteger(payload.q) && payload.q >= 0 &&
    typeof payload.f === 'string' &&
    typeof payload.t === 'string' &&
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
      `时间范围起点已改变（会话发起时为 ${payload.f}），请用原始范围或开启新快照会话`
    );
  }
  if (toTs != null && toTs !== payload.t) {
    throw new CursorError(
      'cursor_range_mismatch',
      `时间范围终点已改变（会话发起时为 ${payload.t}），请用原始范围或开启新快照会话`
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
