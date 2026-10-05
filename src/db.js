import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

/**
 * 持久化层（node:sqlite，Node 22 内置，同步 API；WAL 模式，崩溃后不损坏）。
 *
 * 数据模型：
 *  - samples(stream_id, sample_id, ts, value, seq)
 *      seq 为全局单调递增的接收序号（写入即分配），(stream_id, sample_id) 唯一。
 *  - seq_meta：单行，保存已分配的最后一个接收序号（= 快照上界来源）。
 */
export class Store {
  constructor(dbPath) {
    if (dbPath !== ':memory:') mkdirSync(dirname(dbPath), { recursive: true });
    this.db = new DatabaseSync(dbPath);
    this.db.exec('PRAGMA journal_mode=WAL');
    this.db.exec('PRAGMA foreign_keys=ON');
    this.db.exec('PRAGMA busy_timeout=5000');
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS samples (
        stream_id TEXT    NOT NULL,
        sample_id TEXT    NOT NULL,
        ts        TEXT    NOT NULL,   -- RFC3339，字典序即时间序（要求 UTC / 'Z'）
        value     INTEGER NOT NULL,
        seq       INTEGER NOT NULL,
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
    this.#prepareStatements();
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
        'INSERT INTO samples (stream_id, sample_id, ts, value, seq) VALUES (?, ?, ?, ?, ?)'
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
        insert.run(streamId, it.sampleId, it.ts, it.value, seq);
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
