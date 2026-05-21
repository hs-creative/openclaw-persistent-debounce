import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
import { createPersistentDebouncePlugin } from "./src/plugin.js";

export default definePluginEntry({
  id: "persistent-debounce",
  name: "Persistent Debounce",
  description: "SQLite-backed inbound debounce queue that survives gateway restarts without duplicate replies.",
  register(api) {
    const config = api.pluginConfig ?? {};
    createPersistentDebouncePlugin(api, config);

    api.registerTool({
      name: "debounce_queue_status",
      description: "Check the persistent debounce queue status for a channel and account.",
      parameters: {
        type: "object",
        properties: {
          channel: { type: "string", description: "Channel name, e.g. whatsapp" },
          accountId: { type: "string", description: "Account ID, e.g. lisa or default" },
        },
        required: ["channel"],
      },
      async execute(_id, params) {
        const { channel, accountId } = params;
        const { PersistentDebouncer } = await import("./src/debouncer.js");
        const config = api.pluginConfig ?? {};
        const dbPath = config.dbPath;
        const debouncer = new PersistentDebouncer(dbPath);
        try {
          const status = debouncer.getQueueStatus(channel, accountId ?? null);
          return {
            content: [
              {
                type: "text",
                text: JSON.stringify({ channel, accountId: accountId ?? null, status }, null, 2),
              },
            ],
          };
        } finally {
          debouncer.close();
        }
      },
    });
  },
});
