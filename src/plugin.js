import { PersistentDebouncer } from "./debouncer.js";

function resolveAgentId(cfg) {
  const agents = cfg.agents;
  if (agents?.defaults?.id) return agents.defaults.id;
  if (agents?.list?.[0]?.id) return agents.list[0].id;
  return "main";
}

function buildSessionKey({ agentId, channel, accountId, senderId }) {
  return `agent:${agentId}:${channel}:direct:${senderId}`;
}

function buildPromptFromItems(items) {
  if (items.length === 1) {
    return items[0].message_text ?? "";
  }
  const parts = items.map((item, i) => `[${i + 1}] ${item.message_text ?? ""}`);
  return parts.join("\n");
}

export function createPersistentDebouncePlugin(api, config) {
  const dbPath = config.dbPath;
  const channelsConfig = config.channels ?? {};
  const flushIntervalMs = config.flushIntervalMs ?? 5000;
  const maxPendingAgeMs = config.maxPendingAgeMs ?? 3600000;

  const debouncer = new PersistentDebouncer(dbPath);
  let flushTimer = null;
  let isShuttingDown = false;

  function resolveChannelConfig(channel, accountId) {
    const chanCfg = channelsConfig[channel];
    if (!chanCfg) return null;
    if (chanCfg.enabled === false) return null;
    if (Array.isArray(chanCfg.accounts) && chanCfg.accounts.length > 0) {
      if (!chanCfg.accounts.includes(accountId)) return null;
    }
    return {
      debounceMs: chanCfg.debounceMs ?? 60000,
    };
  }

  async function flushPending() {
    if (isShuttingDown) return;

    const now = Date.now();
    const readyItems = debouncer.getReadyToFlush(now);
    if (readyItems.length === 0) return;

    const bySession = new Map();
    for (const item of readyItems) {
      const agentId = resolveAgentId(api.config);
      const sessionKey = buildSessionKey({
        agentId,
        channel: item.channel,
        accountId: item.account_id ?? "default",
        senderId: item.sender_id,
      });

      if (!bySession.has(sessionKey)) {
        bySession.set(sessionKey, []);
      }
      bySession.get(sessionKey).push(item);
    }

    for (const [sessionKey, items] of bySession) {
      const conversationId = items[0].conversation_id;
      const channel = items[0].channel;
      const accountId = items[0].account_id ?? "default";

      const ids = items.map((i) => i.id);

      const allMarked = ids.every((id) => debouncer.markFlushing(id));
      if (!allMarked) {
        api.logger.warn("[persistent-debounce] Some items were already flushing, skipping batch", { sessionKey });
        continue;
      }

      const prompt = buildPromptFromItems(items);
      if (!prompt.trim()) {
        for (const id of ids) debouncer.markAnswered(id);
        continue;
      }

      try {
        api.logger.info("[persistent-debounce] Flushing batch to session", { sessionKey, count: items.length });

        const result = await api.runtime.agent.runEmbeddedAgent({
          sessionId: sessionKey,
          runId: crypto.randomUUID(),
          sessionFile: undefined,
          workspaceDir: api.runtime.agent.resolveAgentWorkspaceDir(api.config),
          prompt,
          timeoutMs: api.runtime.agent.resolveAgentTimeoutMs(api.config),
        });

        for (const id of ids) {
          debouncer.markAnswered(id);
        }

        api.logger.info("[persistent-debounce] Batch flushed successfully", { sessionKey, resultLength: result?.content?.length });
      } catch (err) {
        api.logger.error("[persistent-debounce] Flush failed for session", { sessionKey, error: err.message });

        for (const id of ids) {
          debouncer.resetToPending(id);
        }
      }
    }
  }

  function startFlushTimer() {
    if (flushTimer) clearInterval(flushTimer);
    flushTimer = setInterval(() => {
      flushPending().catch((err) => {
        api.logger.error("[persistent-debounce] Flush timer error", { error: err.message });
      });
    }, flushIntervalMs);
    flushTimer.unref?.();
  }

  function stopFlushTimer() {
    if (flushTimer) {
      clearInterval(flushTimer);
      flushTimer = null;
    }
  }

  function doRecovery() {
    const now = Date.now();
    const agentId = resolveAgentId(api.config);

    for (const [channel, chanCfg] of Object.entries(channelsConfig)) {
      if (chanCfg.enabled === false) continue;
      const accounts = chanCfg.accounts ?? [null];
      for (const accountId of accounts) {
        const pending = debouncer.getPendingForRecovery(channel, accountId, maxPendingAgeMs, now);
        if (pending.length === 0) continue;

        api.logger.info("[persistent-debounce] Recovery found pending messages", {
          channel,
          accountId,
          count: pending.length,
        });
      }
    }
  }

  api.on("gateway_start", async () => {
    api.logger.info("[persistent-debounce] Gateway start — initializing queue");
    isShuttingDown = false;
    doRecovery();
    startFlushTimer();
  });

  api.on("gateway_stop", async () => {
    api.logger.info("[persistent-debounce] Gateway stop — cleaning up");
    isShuttingDown = true;
    stopFlushTimer();
    try {
      await flushPending();
    } catch (err) {
      api.logger.error("[persistent-debounce] Final flush failed", { error: err.message });
    }
    debouncer.close();
  });

  api.on("message_received", async (event) => {
    const channel = event.channel ?? event.metadata?.channel;
    const accountId = event.accountId ?? event.metadata?.accountId ?? "default";
    const senderId = event.senderId ?? event.metadata?.senderId;
    const conversationId = event.conversationId ?? event.threadId ?? senderId;

    if (!channel || !senderId) return;

    const chanCfg = resolveChannelConfig(channel, accountId);
    if (!chanCfg) return;

    const receivedAt = Date.now();
    const result = debouncer.enqueue({
      channel,
      accountId,
      senderId,
      conversationId,
      text: event.content?.text ?? event.text ?? "",
      receivedAt,
      debounceMs: chanCfg.debounceMs,
      metadata: {
        messageId: event.messageId,
        threadId: event.threadId,
        replyToId: event.replyToId,
        timestamp: event.timestamp,
      },
    });

    if (result.isDuplicate) {
      api.logger.debug("[persistent-debounce] Deduplicated duplicate message", { channel, senderId, messageId: event.messageId });
      return { cancel: true, cancelReason: "persistent_debounce_duplicate" };
    }

    api.logger.info("[persistent-debounce] Queued message for debounce", {
      channel,
      senderId,
      debounceMs: chanCfg.debounceMs,
      id: result.id,
    });

    return { cancel: true, cancelReason: "persistent_debounce_queued" };
  });

  api.on("message_sending", async (event) => {
    const sessionKey = event.context?.sessionKey;
    if (!sessionKey) return;

    const agentId = resolveAgentId(api.config);
    const prefix = `agent:${agentId}:`;
    if (!sessionKey.startsWith(prefix)) return;

    const rest = sessionKey.slice(prefix.length);
    const [channel, chatType, senderId] = rest.split(":");
    if (!channel || !senderId) return;

    const chanCfg = channelsConfig[channel];
    if (!chanCfg || chanCfg.enabled === false) return;

    try {
      const status = debouncer.getQueueStatus(channel, null);
      const flushingCount = status.find((s) => s.status === "flushing")?.count ?? 0;
      if (flushingCount > 0) {
        api.logger.info("[persistent-debounce] Outbound reply detected, marking conversation answered", { sessionKey });
      }
    } catch (err) {
      api.logger.error("[persistent-debounce] Error marking answered", { error: err.message });
    }
  });

  return {
    debouncer,
    flushPending,
    startFlushTimer,
    stopFlushTimer,
  };
}
