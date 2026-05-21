import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import test from "node:test";
import { PersistentDebouncer } from "../src/debouncer.js";

function withTempDb(fn) {
  const dir = mkdtempSync(join(tmpdir(), "persistent-debounce-"));
  const dbPath = join(dir, "queue.db");
  try {
    return fn(dbPath);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

test("persists queued messages and recovers them after reopen", () => {
  withTempDb((dbPath) => {
    const receivedAt = Date.now() - 1000;
    const first = new PersistentDebouncer(dbPath);
    const result = first.enqueue({
      channel: "whatsapp",
      accountId: "lisa",
      senderId: "+49123456789",
      conversationId: "+49123456789",
      text: "Bitte spaeter antworten",
      receivedAt,
      debounceMs: 500,
      metadata: { messageId: "wa-1" },
    });

    assert.equal(result.success, true);
    assert.equal(result.isDuplicate, false);
    first.close();

    const second = new PersistentDebouncer(dbPath);
    const ready = second.getReadyToFlush(Date.now());
    assert.equal(ready.length, 1);
    assert.equal(ready[0].message_text, "Bitte spaeter antworten");
    assert.equal(ready[0].status, "pending");
    second.close();
  });
});

test("deduplicates redelivered messages in the same time bucket", () => {
  withTempDb((dbPath) => {
    const db = new PersistentDebouncer(dbPath);
    const item = {
      channel: "whatsapp",
      accountId: "lisa",
      senderId: "+49123456789",
      conversationId: "+49123456789",
      text: "Nur einmal antworten",
      receivedAt: 1770000000123,
      debounceMs: 1000,
      metadata: { messageId: "wa-2" },
    };

    assert.equal(db.enqueue(item).isDuplicate, false);
    assert.equal(db.enqueue({ ...item, metadata: { messageId: "wa-2-redelivered" } }).isDuplicate, true);

    const status = db.getQueueStatus("whatsapp", "lisa");
    assert.deepEqual(status, [{ status: "pending", count: 1 }]);
    db.close();
  });
});
