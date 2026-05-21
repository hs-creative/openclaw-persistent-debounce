import Database from "better-sqlite3";
import { createHash } from "crypto";
import { mkdirSync, dirname } from "fs";
import { join } from "path";
import { homedir } from "os";

function sha256(input) {
  return createHash("sha256").update(input).digest("hex");
}

function buildDedupeKey({ channel, accountId, senderId, conversationId, text, receivedAt }) {
  const timeBucket = Math.floor(receivedAt / 1000);
  const payload = `${channel}|${accountId ?? ""}|${senderId}|${conversationId}|${text ?? ""}|${timeBucket}`;
  return sha256(payload);
}

export class PersistentDebouncer {
  constructor(dbPath) {
    this.dbPath = dbPath ?? join(homedir(), ".openclaw", "persistent-debounce.db");
    mkdirSync(dirname(this.dbPath), { recursive: true });
    this.db = new Database(this.dbPath);
    this.db.pragma("journal_mode = WAL");
    this._initSchema();
  }

  _initSchema() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS inbound_queue (
        id TEXT PRIMARY KEY,
        dedupe_key TEXT NOT NULL UNIQUE,
        channel TEXT NOT NULL,
        account_id TEXT,
        sender_id TEXT NOT NULL,
        conversation_id TEXT NOT NULL,
        message_text TEXT,
        message_json TEXT NOT NULL,
        received_at INTEGER NOT NULL,
        scheduled_flush_at INTEGER NOT NULL,
        flushed_at INTEGER,
        answered_at INTEGER,
        status TEXT NOT NULL DEFAULT 'pending',
        attempt_count INTEGER NOT NULL DEFAULT 0
      );

      CREATE INDEX IF NOT EXISTS idx_status_flush ON inbound_queue(status, scheduled_flush_at);
      CREATE INDEX IF NOT EXISTS idx_channel_account ON inbound_queue(channel, account_id);
      CREATE INDEX IF NOT EXISTS idx_dedupe ON inbound_queue(dedupe_key);
    `);
  }

  enqueue(item) {
    const dedupeKey = buildDedupeKey(item);
    const id = sha256(dedupeKey + Date.now().toString());
    const scheduledFlushAt = item.receivedAt + (item.debounceMs ?? 60000);

    try {
      const insert = this.db.prepare(`
        INSERT INTO inbound_queue (
          id, dedupe_key, channel, account_id, sender_id, conversation_id,
          message_text, message_json, received_at, scheduled_flush_at, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
      `);
      insert.run(
        id,
        dedupeKey,
        item.channel,
        item.accountId ?? null,
        item.senderId,
        item.conversationId,
        item.text ?? null,
        JSON.stringify(item.metadata ?? {}),
        item.receivedAt,
        scheduledFlushAt
      );
      return { success: true, id, isDuplicate: false };
    } catch (err) {
      if (err.message?.includes("UNIQUE constraint failed") || err.message?.includes("duplicate")) {
        return { success: true, id: null, isDuplicate: true };
      }
      throw err;
    }
  }

  getReadyToFlush(now = Date.now()) {
    const stmt = this.db.prepare(`
      SELECT * FROM inbound_queue
      WHERE status = 'pending' AND scheduled_flush_at <= ?
      ORDER BY received_at ASC
    `);
    return stmt.all(now);
  }

  getPendingForRecovery(channel, accountId, maxAgeMs = 3600000, now = Date.now()) {
    const cutoff = now - maxAgeMs;
    const stmt = this.db.prepare(`
      SELECT * FROM inbound_queue
      WHERE channel = ? AND account_id = ?
        AND status IN ('pending', 'flushing')
        AND received_at >= ?
      ORDER BY received_at ASC
    `);
    return stmt.all(channel, accountId ?? null, cutoff);
  }

  markFlushing(id) {
    const stmt = this.db.prepare(`
      UPDATE inbound_queue
      SET status = 'flushing', flushed_at = ?, attempt_count = attempt_count + 1
      WHERE id = ? AND status = 'pending'
    `);
    const result = stmt.run(Date.now(), id);
    return result.changes > 0;
  }

  markAnswered(id) {
    const stmt = this.db.prepare(`
      UPDATE inbound_queue
      SET status = 'answered', answered_at = ?
      WHERE id = ?
    `);
    stmt.run(Date.now(), id);
  }

  markFailed(id) {
    const stmt = this.db.prepare(`
      UPDATE inbound_queue
      SET status = 'failed'
      WHERE id = ?
    `);
    stmt.run(id);
  }

  resetToPending(id) {
    const stmt = this.db.prepare(`
      UPDATE inbound_queue
      SET status = 'pending', flushed_at = NULL
      WHERE id = ?
    `);
    stmt.run(id);
  }

  getQueueStatus(channel, accountId) {
    const stmt = this.db.prepare(`
      SELECT
        status,
        COUNT(*) as count
      FROM inbound_queue
      WHERE channel = ? AND account_id = ?
      GROUP BY status
    `);
    return stmt.all(channel, accountId ?? null);
  }

  pruneOld(maxAgeMs = 86400000, now = Date.now()) {
    const cutoff = now - maxAgeMs;
    const stmt = this.db.prepare(`
      DELETE FROM inbound_queue
      WHERE status IN ('answered', 'failed') AND answered_at < ?
    `);
    const result = stmt.run(cutoff);
    return result.changes;
  }

  close() {
    this.db.close();
  }
}
