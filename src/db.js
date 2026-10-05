import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { bucketOf } from './shard.js';

/**
 * 持久化层（node:sqlite，Node 22 内置，同步 API；WAL 模式，崩溃后不损坏）。
 *
 * 数据模型：
 *  - samples(stream_id, sample_id, ts, value, seq, bucket)
 *      seq 为全局单调递增的接收序号（写入即分配），(stream_id, sample_id) 唯一。
 *      bucket = sha256(ts ‖ sampleId) % 8，写入时确定，供并行导出分片使用。
 *  - seq_meta：单行，保存已分配的最后一个接收序号（= 快照上界来源）。
 */
export class Store {
  constructor(dbPath) {
    if (dbPath !== ':memory:') mkdirSync(dirname(dbPath), { recursive: true });
    this.db = new DatabaseSync(dbPath);
    this.db.exec('PRAGMA journal_mode=WAL');
    this.db.exec('PRAGMA foreign_keys=ON');
    this.db.exec('PRAGMA busy_timeout=5000');
    this.#createSchema();
    this.#migrateAddBucket();
    // 迁移会丢弃旧表上的索引，统一在迁移后确保两个索引都存在
    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_samples_stream_ts
        ON samples (stream_id, ts, sample_id)
    `);
    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_samples_stream_bucket
        ON samples (stream_id, bucket, ts, sample_id)
    `);
    this.#prepareStatements();
    this.shardStmtCache = new Map();
  }

  #createSchema() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS samples (
        stream_id TEXT    NOT NULL,
        sample_id TEXT    NOT NULL,
        ts        TEXT    NOT NULL,   -- RFC3339，字典序即时间序（要求 UTC / 'Z'）
        value     INTEGER NOT NULL,
        seq       INTEGER NOT NULL,
        bucket    INTEGER NOT NULL,   -- sha256(ts ‖ sampleId) % 8，并行导出分片键
        PRIMARY KEY (stream_id, sample_id)
      ) WITHOUT ROWID
    `);
    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_samples_stream_ts
        ON samples (stream_id, ts, sample_id)
    `);
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS seq_meta (
        id     INTEGER PRIMARY KEY CHECK (id = 1),
        last_seq INTEGER NOT NULL
      )
    `);
    this.db.exec(`
      INSERT OR IGNORE INTO seq_meta (id, last_seq) VALUES (1, 0)
    `);
  }

  /**
   * 旧库（无 bucket 列）迁移：重建表并回填哈希桶。
   * WITHOUT ROWID 表的 ALTER TABLE ADD COLUMN 支持随 SQLite 版本而异，
   * 故统一走“建新表 -> JS 回填 -> 换名”的确定性路径。
   */
  #migrateAddBucket() {
    const db = this.db;
    const cols = db.prepare('PRAGMA table_info(samples)').all();
    if (cols.some((c) => c.name === 'bucket')) return;

    db.exec('BEGIN IMMEDIATE');
    try {
      db.exec('ALTER TABLE samples RENAME TO samples_legacy');
      db.exec(`
        CREATE TABLE samples (
          stream_id TEXT    NOT NULL,
          sample_id TEXT    NOT NULL,
          ts        TEXT    NOT NULL,
          value     INTEGER NOT NULL,
          seq       INTEGER NOT NULL,
          bucket    INTEGER NOT NULL,
          PRIMARY KEY (stream_id, sample_id)
        ) WITHOUT ROWID
      `);
      const legacy = db.prepare(
        'SELECT stream_id, sample_id, ts, value, seq FROM samples_legacy'
      ).all();
      const ins = db.prepare(
        'INSERT INTO samples (stream_id, sample_id, ts, value, seq, bucket) ' +
        'VALUES (?, ?, ?, ?, ?, ?)'
      );
      for (const r of legacy) {
        ins.run(r.stream_id, r.sample_id, r.ts, r.value, Number(r.seq),
          bucketOf(r.ts, r.sample_id));
      }
      db.exec('DROP TABLE samples_legacy');
      db.exec(`
        CREATE INDEX IF NOT EXISTS idx_samples_stream_bucket
          ON samples (stream_id, bucket, ts, sample_id)
      `);
      db.exec('COMMIT');
    } catch (err) {
      db.exec('ROLLBACK');
      throw err;
    }
  }

  #prepareStatements() {
    const db = this.db;
    this.stmts = {
      getLastSeq: db.prepare('SELECT last_seq FROM seq_meta WHERE id = 1'),
      bumpSeq: db.prepare('UPDATE seq_meta SET last_seq = ? WHERE id = 1'),
      existsDup: db.prepare(
        'SELECT 1 FROM samples WHERE stream_id = ? AND sample_id = ?'
      ),
      insert: db.prepare(
        'INSERT INTO samples (stream_id, sample_id, ts, value, seq, bucket) VALUES (?, ?, ?, ?, ?, ?)'
      ),
      pageStart: db.prepare(
        `SELECT ts, sample_id AS sampleId, value, seq
           FROM samples
          WHERE stream_id = ?
            AND seq <= ?
            AND ts >= ? AND ts <= ?
          ORDER BY ts ASC, sample_id ASC
          LIMIT ?`
      ),
      pageAfter: db.prepare(
        `SELECT ts, sample_id AS sampleId, value, seq
           FROM samples
          WHERE stream_id = ?
            AND seq <= ?
            AND ts >= ? AND ts <= ?
            AND (ts > ? OR (ts = ? AND sample_id > ?))
          ORDER BY ts ASC, sample_id ASC
          LIMIT ?`
      ),
      countSnapshot: db.prepare(
        `SELECT COUNT(*) AS n FROM samples
          WHERE stream_id = ? AND seq <= ? AND ts >= ? AND ts <= ?`
      ),
      totalSamples: db.prepare('SELECT COUNT(*) AS n FROM samples'),
    };
  }

  /**
   * 分片页查询：bucket 列表随 worker 数变化（每 worker 1..4 个桶），
   * 按桶数量缓存预编译语句。
   */
  #shardStatement(kind, bucketCount) {
    const key = `${kind}:${bucketCount}`;
    let stmt = this.shardStmtCache.get(key);
    if (stmt) return stmt;
    const inList = Array.from({ length: bucketCount }, () => '?').join(',');
    const sql =
      `SELECT ts, sample_id AS sampleId, value, seq, bucket
         FROM samples
        WHERE stream_id = ?
          AND seq <= ?
          AND ts >= ? AND ts <= ?
          AND bucket IN (${inList})` +
      (kind === 'after'
        ? `
          AND (bucket > ?
               OR (bucket = ? AND (ts > ? OR (ts = ? AND sample_id > ?))))
          ORDER BY bucket ASC, ts ASC, sample_id ASC
          LIMIT ?`
        : `
          ORDER BY bucket ASC, ts ASC, sample_id ASC
          LIMIT ?`);
    stmt = this.db.prepare(sql);
    this.shardStmtCache.set(key, stmt);
    return stmt;
  }

  /** 当前已分配的最后接收序号（同时也是“最新快照”的上界）。 */
  currentSeq() {
    return Number(this.stmts.getLastSeq.get().last_seq);
  }

  /**
   * 原子批量写入（1..100 条）。
   * 批内重复、或与已存在的 sampleId 冲突 -> 抛 BatchConflictError，整批回滚，不留部分写入。
   * 返回 { firstSeq, lastSeq }。
   */
  insertBatch(streamId, items) {
    const db = this.db;
    const insert = this.stmts.insert;
    const existsDup = this.stmts.existsDup;
    const bumpSeq = this.stmts.bumpSeq;

    db.exec('BEGIN IMMEDIATE');
    try {
      let seq = Number(this.stmts.getLastSeq.get().last_seq);
      const firstSeq = seq + 1;

      // 批内唯一性预检（尽早失败，且避免唯一约束报错时占用 seq 号段）
      const seen = new Set();
      for (const it of items) {
        if (seen.has(it.sampleId)) {
          throw new BatchConflictError(
            `批内 sampleId 重复: ${it.sampleId}`,
            { reason: 'duplicate_in_batch', sampleId: it.sampleId }
          );
        }
        seen.add(it.sampleId);
      }

      for (const it of items) {
        if (existsDup.get(streamId, it.sampleId)) {
          throw new BatchConflictError(
            `sampleId 已存在: ${it.sampleId}`,
            { reason: 'conflict_existing', sampleId: it.sampleId }
          );
        }
        seq += 1;
        insert.run(streamId, it.sampleId, it.ts, it.value, seq,
          bucketOf(it.ts, it.sampleId));
      }
      bumpSeq.run(seq);
      db.exec('COMMIT');
      return { firstSeq, lastSeq: seq, count: items.length };
    } catch (err) {
      db.exec('ROLLBACK');
      throw err;
    }
  }

  /**
   * 读取快照页（稳定排序：ts ASC, sampleId ASC）。
   * @param {string} streamId
   * @param {number} snapshotSeq 快照上界（只含 seq <= snapshotSeq）
   * @param {{fromTs:string,toTs:string}} range 闭区间，按 ts 字符串比较
   * @param {{ts:string,sampleId:string}|null} after 严格大于该位置之后开始
   * @param {number} pageSize
   */
  readPage(streamId, snapshotSeq, range, after, pageSize) {
    // keyset 分页：比较 (ts, sample_id) 元组，稳定且不依赖 OFFSET
    let rows;
    if (after) {
      rows = this.stmts.pageAfter.all(
        streamId, snapshotSeq, range.fromTs, range.toTs,
        after.ts, after.ts, after.sampleId, pageSize
      );
    } else {
      rows = this.stmts.pageStart.all(
        streamId, snapshotSeq, range.fromTs, range.toTs, pageSize
      );
    }
    return rows.map((r) => ({
      ts: r.ts,
      sampleId: r.sampleId,
      value: Number(r.value),
      seq: Number(r.seq),
    }));
  }

  /**
   * 读取某导出分片的一页（按 bucket ASC, ts ASC, sampleId ASC 稳定排序）。
   *
   * @param {string} streamId
   * @param {number} snapshotSeq 快照上界（只含 seq <= snapshotSeq）
   * @param {{fromTs:string,toTs:string}} range 闭区间
   * @param {number[]} buckets 该 worker 负责的桶
   * @param {{bucket:number,ts:string,sampleId:string}|null} after 严格大于该位置
   * @param {number} pageSize
   */
  readShardPage(streamId, snapshotSeq, range, buckets, after, pageSize) {
    const limit = pageSize + 1; // 多取一条以判断是否还有下一页
    const args = [streamId, snapshotSeq, range.fromTs, range.toTs, ...buckets];
    let rows;
    if (after) {
      const stmt = this.#shardStatement('after', buckets.length);
      rows = stmt.all(
        ...args,
        after.bucket, after.bucket, after.ts, after.ts, after.sampleId,
        limit
      );
    } else {
      const stmt = this.#shardStatement('start', buckets.length);
      rows = stmt.all(...args, limit);
    }
    return rows.map((r) => ({
      ts: r.ts,
      sampleId: r.sampleId,
      value: Number(r.value),
      seq: Number(r.seq),
      bucket: Number(r.bucket),
    }));
  }

  countSnapshot(streamId, snapshotSeq, range) {
    return Number(
      this.stmts.countSnapshot.get(streamId, snapshotSeq, range.fromTs, range.toTs).n
    );
  }

  totalSamples() {
    return Number(this.stmts.totalSamples.get().n);
  }

  close() {
    this.db.close();
  }
}

export class BatchConflictError extends Error {
  constructor(message, details) {
    super(message);
    this.name = 'BatchConflictError';
    this.statusCode = 409;
    this.details = details;
  }
}
