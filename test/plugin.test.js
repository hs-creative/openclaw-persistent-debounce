import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import test from "node:test";
import { createPersistentDebouncePlugin } from "../src/plugin.js";

function createApiStub() {
  const handlers = new Map();
  const runs = [];
  return {
    api: {
      config: { agents: { defaults: { id: "main" } } },
      logger: {
        debug() {},
        info() {},
        warn() {},
        error() {},
      },
      runtime: {
        agent: {
          resolveAgentWorkspaceDir() {
            return "/tmp";
          },
          resolveAgentTimeoutMs() {
            return 1000;
          },
          async runEmbeddedAgent(payload) {
            runs.push(payload);
            return { content: "ok" };
          },
        },
      },
      on(event, handler) {
        handlers.set(event, handler);
      },
    },
    handlers,
    runs,
  };
}

test("queues inbound messages and flushes a single embedded agent run", async () => {
  const dir = mkdtempSync(join(tmpdir(), "persistent-debounce-plugin-"));
  try {
    const dbPath = join(dir, "queue.db");
    const { api, handlers, runs } = createApiStub();
    const plugin = createPersistentDebouncePlugin(api, {
      dbPath,
      channels: {
        whatsapp: {
          enabled: true,
          debounceMs: 0,
          accounts: ["lisa"],
        },
      },
      flushIntervalMs: 1000,
    });

    const result = await handlers.get("message_received")({
      channel: "whatsapp",
      accountId: "lisa",
      senderId: "+49123456789",
      conversationId: "+49123456789",
      content: { text: "Testnachricht" },
      messageId: "wa-3",
    });

    assert.deepEqual(result, { cancel: true, cancelReason: "persistent_debounce_queued" });

    await plugin.flushPending();

    assert.equal(runs.length, 1);
    assert.equal(runs[0].sessionId, "agent:main:whatsapp:direct:+49123456789");
    assert.equal(runs[0].prompt, "Testnachricht");
    assert.deepEqual(plugin.debouncer.getQueueStatus("whatsapp", "lisa"), [{ status: "answered", count: 1 }]);
    plugin.debouncer.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
