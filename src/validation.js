/**
 * 请求校验。时间戳要求严格 RFC3339 且带偏移（统一归一化为 UTC 的 'Z' 形式，
 * 这样字符串字典序即时间序）。
 */

export class ValidationError extends Error {
  constructor(message, details) {
    super(message);
    this.name = 'ValidationError';
    this.statusCode = 400;
    this.details = details;
  }
}

// 严格 RFC3339，例如 2026-10-05T08:30:00Z / 2026-10-05T08:30:00.123456789+08:00
const RFC3339_RE =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/;

export function parseRfc3339(input) {
  if (typeof input !== 'string' || !RFC3339_RE.test(input)) {
    throw new ValidationError(`非法 RFC3339 时间戳: ${JSON.stringify(input)}`);
  }
  const d = new Date(input);
  if (Number.isNaN(d.getTime())) {
    throw new ValidationError(`非法日历时间: ${input}`);
  }
  return { date: d, canonical: toCanonicalUtc(input, d) };
}

/** 归一化为 UTC 规范串：YYYY-MM-DDTHH:mm:ss.fffffffffZ（小数秒固定 9 位）。
 *  固定宽度后，字符串字典序严格等于时间序，且同一时刻只有唯一表示。 */
function toCanonicalUtc(input, d) {
  const fracMatch = input.match(/\.(\d{1,9})/);
  const frac = fracMatch ? fracMatch[1].padEnd(9, '0') : '0'.repeat(9);
  const pad = (n, w = 2) => String(n).padStart(w, '0');
  return (
    `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}` +
    `T${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}` +
    `.${frac}Z`
  );
}

const MIN_TS = '0000-01-01T00:00:00.000000000Z';   // 合法 ts 字典序下界
const MAX_TS = '9999-12-31T23:59:59.999999999Z';   // 字典序上界

/**
 * 校验并归一化一批样本。
 * @returns {{items: Array<{sampleId:string,ts:string,value:number}>}}
 */
export function validateBatch(body) {
  if (body === null || typeof body !== 'object' || Array.isArray(body)) {
    throw new ValidationError('请求体必须为对象，且含 samples 数组');
  }
  const samples = body.samples;
  if (!Array.isArray(samples)) {
    throw new ValidationError('字段 samples 必须为数组');
  }
  if (samples.length < 1 || samples.length > 100) {
    throw new ValidationError('每批接收 1 至 100 条观测', {
      received: samples.length,
    });
  }

  const items = samples.map((s, idx) => {
    const where = `samples[${idx}]`;
    if (s === null || typeof s !== 'object' || Array.isArray(s)) {
      throw new ValidationError(`${where} 必须为对象`);
    }
    const { sampleId, ts, value } = s;

    if (typeof sampleId !== 'string' || sampleId.length === 0) {
      throw new ValidationError(`${where}.sampleId 必须为非空字符串`);
    }
    if (sampleId.length > 200) {
      throw new ValidationError(`${where}.sampleId 过长（最多 200 字符）`);
    }

    const { canonical } = parseRfc3339(ts);

    // 整数读数：拒绝 1.5、"3"、true、NaN/Infinity，以及超出安全整数范围的值
    if (typeof value !== 'number' || !Number.isSafeInteger(value)) {
      throw new ValidationError(`${where}.value 必须为整数（安全整数范围内）`);
    }

    return { sampleId, ts: canonical, value };
  });

  return { items };
}

/**
 * 校验查询参数。from/to 可缺省（缺省用 MIN/MAX）；cursor 存在时时间范围由游标会话决定。
 *
 * 普通快照分页：from/to/pageSize/cursor。
 * 并行导出分片：exportToken（必填）、workerIndex（零基，必填）、pageSize、cursor
 * （游标为该分片的续页游标）。未携带 exportToken 时一切行为与旧版兼容。
 */
export function validateQuery(searchParams) {
  const out = { rawFrom: null, rawTo: null, fromTs: MIN_TS, toTs: MAX_TS };

  const from = searchParams.get('from');
  const to = searchParams.get('to');
  if (from !== null) {
    out.rawFrom = from;
    out.fromTs = parseRfc3339(from).canonical;
  }
  if (to !== null) {
    out.rawTo = to;
    out.toTs = parseRfc3339(to).canonical;
  }
  if (out.fromTs > out.toTs) {
    throw new ValidationError('时间范围非法：from 晚于 to');
  }

  let pageSize = 100;
  const ps = searchParams.get('pageSize');
  if (ps !== null) {
    if (!/^\d+$/.test(ps)) {
      throw new ValidationError('pageSize 必须为正整数');
    }
    pageSize = Number(ps);
    if (pageSize < 1 || pageSize > 1000) {
      throw new ValidationError('pageSize 允许范围为 1..1000');
    }
  }
  out.pageSize = pageSize;
  out.cursor = searchParams.get('cursor');

  out.exportToken = searchParams.get('exportToken');
  const wi = searchParams.get('workerIndex');
  if (wi !== null) {
    if (!/^\d+$/.test(wi)) {
      throw new ValidationError('workerIndex 必须为零基非负整数');
    }
    out.workerIndex = Number(wi);
  } else {
    out.workerIndex = null;
  }
  return out;
}

/**
 * 校验导出创建请求体（POST .../exports）。
 * @returns {{fromTs:string, toTs:string, workerCount:number}}
 */
export function validateExport(body) {
  if (body === null || typeof body !== 'object' || Array.isArray(body)) {
    throw new ValidationError('请求体必须为对象');
  }

  let fromTs = MIN_TS;
  let toTs = MAX_TS;
  if (body.from !== undefined && body.from !== null) {
    fromTs = parseRfc3339(body.from).canonical;
  }
  if (body.to !== undefined && body.to !== null) {
    toTs = parseRfc3339(body.to).canonical;
  }
  if (fromTs > toTs) {
    throw new ValidationError('时间范围非法：from 晚于 to');
  }

  // workerCount 必填，允许 2..8
  const wc = body.workerCount;
  if (typeof wc !== 'number' || !Number.isInteger(wc) || wc < 2 || wc > 8) {
    throw new ValidationError('workerCount 必须为 2..8 的整数', {
      received: wc,
    });
  }

  return { fromTs, toTs, workerCount: wc };
}

export const TIME_BOUNDS = { MIN_TS, MAX_TS };
