import { createHmac, timingSafeEqual } from 'node:crypto';

/**
 * 不透明令牌 / 分页游标。
 *
 * 线格式统一为：<payloadB64>.<sigB64>，签名为 HMAC-SHA256(secret, payloadB64)。
 *
 * 1) 普通快照分页游标（v=1）：
 *   { v:1, s:streamId, q:snapshotSeq, f:fromTs, t:toTs, p:pageSize,
 *     a:{ts, sampleId}|null, e?:exportTokenId, w?:workerIndex }
 *   - 无 e/w：普通快照会话游标。
 *   - 有 e/w：并行导出会话中第 w 个分片的游标，绑定到导出令牌 id。
 *
 * 2) 并行导出令牌（v=2）：
 *   { v:2, d:exportTokenId }
 *   导出会话参数（流、快照序号、时间范围、workerCount）持久化在服务端，
 *   令牌本身不携带任何可读信息，只能原样回传。
 *
 * 安全性质：
 *  - 篡改任一字段 -> 签名校验失败 -> cursor_signature_invalid / export_token_invalid
 *  - 跨流复用、工作进程越界、分片游标串用 -> 由应用比对 -> 明确可区分的 4xx
 *  - 不透明：客户端无法构造，也读不出快照参数
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
  if (!isValidPayload(payload)) {
    throw new CursorError('invalid_cursor', '游标载荷非法');
  }
  return payload;
}

function isValidPayload(payload) {
  if (!payload || typeof payload !== 'object') return false;
  if (payload.v === 2) {
    // 导出令牌：仅含不透明 id
    return typeof payload.d === 'string' && payload.d.length > 0;
  }
  if (payload.v !== 1) return false;
  const baseOk =
    typeof payload.s === 'string' &&
    Number.isInteger(payload.q) && payload.q >= 0 &&
    typeof payload.f === 'string' &&
    typeof payload.t === 'string' &&
    Number.isInteger(payload.p) && payload.p >= 1 &&
    (payload.a === null ||
      (typeof payload.a === 'object' &&
       typeof payload.a.ts === 'string' &&
       typeof payload.a.i === 'string'));
  if (!baseOk) return false;
  // 分片字段只能成对出现
  if (payload.e !== undefined && typeof payload.e !== 'string') return false;
  if (payload.w !== undefined) {
    if (!Number.isInteger(payload.w) || payload.w < 0) return false;
    if (payload.e === undefined) return false;
  }
  return true;
}

/**
 * 校验游标是否可用于当前请求（流、时间范围必须与发起会话时一致）。
 * 返回规范化后的 { snapshotSeq, fromTs, toTs, pageSize, after }。
 * 仅用于普通快照会话（v=1 且无分片绑定）。
 */
export function bindCursor(payload, { streamId, fromTs, toTs, pageSize }) {
  if (payload.v !== 1 || payload.e !== undefined) {
    throw new CursorError('invalid_cursor', '该令牌不是普通快照分页游标');
  }
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

/**
 * 解析并绑定并行导出令牌。
 * @param {object} payload parseCursor 的结果
 * @param {{streamId:string}} ctx
 * @param {(tokenId:string) => object|null} loadExport 按 id 读取持久化的导出会话
 * @returns 导出会话记录（含 snapshotSeq/fromTs/toTs/workerCount）
 */
export function bindExportToken(payload, { streamId }, loadExport) {
  if (payload.v !== 2) {
    throw new CursorError('invalid_export_token', 'exportToken 非法：不是导出令牌');
  }
  const rec = loadExport(payload.d);
  if (!rec) {
    throw new CursorError(
      'export_not_found',
      '导出会话不存在或已失效',
      404
    );
  }
  if (rec.streamId !== streamId) {
    throw new CursorError(
      'export_stream_mismatch',
      '导出令牌属于其他数据流，不能跨流使用'
    );
  }
  return rec;
}

/**
 * 校验分片分页游标与当前导出请求一致。
 * 错误彼此可区分：游标不属于任何分片 / 属于其他导出 / 属于其他分片 /
 * 跨流 / 范围被改。
 * @returns {{snapshotSeq:number, fromTs:string, toTs:string, pageSize:number, after:object|null}}
 */
export function bindShardCursor(payload, { exportRec, tokenId, streamId, workerIndex, pageSize }) {
  if (payload.v !== 1 || payload.e === undefined || payload.w === undefined) {
    throw new CursorError('invalid_cursor', '游标不属于该导出会话的分片');
  }
  if (payload.s !== streamId || payload.s !== exportRec.streamId) {
    throw new CursorError(
      'cursor_stream_mismatch',
      '游标属于其他数据流，不能跨流复用'
    );
  }
  if (payload.e !== tokenId) {
    throw new CursorError(
      'cursor_export_mismatch',
      '游标属于其他导出会话，不能与当前 exportToken 混用'
    );
  }
  if (payload.w !== workerIndex) {
    throw new CursorError(
      'cursor_worker_mismatch',
      `游标属于工作进程 ${payload.w} 的分片，不能用于工作进程 ${workerIndex}`
    );
  }
  if (payload.f !== exportRec.fromTs || payload.t !== exportRec.toTs ||
      payload.q !== exportRec.snapshotSeq) {
    throw new CursorError(
      'cursor_export_mismatch',
      '游标快照参数与导出会话不一致'
    );
  }
  return {
    snapshotSeq: exportRec.snapshotSeq,
    fromTs: exportRec.fromTs,
    toTs: exportRec.toTs,
    pageSize: pageSize ?? payload.p,
    after: payload.a ? { ts: payload.a.ts, sampleId: payload.a.i } : null,
  };
}
