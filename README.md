# openclaw-persistent-debounce

SQLite-backed inbound debounce queue for OpenClaw. Survives gateway restarts without duplicate replies or lost messages.

## Problem

OpenClaw's built-in inbound debouncer holds pending messages in memory. If the gateway or server restarts while messages are still in the debounce window, those messages are **lost** — the sender never gets a reply. Additionally, after a crash recovery, the same message may be delivered again by the messaging platform (e.g. WhatsApp Web), causing **duplicate replies**.

## Solution

This plugin replaces the in-memory debounce with a **persistent SQLite queue** that:

1. Stores every inbound message on disk immediately
2. Deduplicates by content hash (prevents double-processing on redelivery)
3. Flushes messages after the configured debounce delay via `runEmbeddedAgent`
4. Recovers pending messages automatically on gateway startup
5. Marks conversations as answered when an outbound reply is detected

## Installation

### 1. Install the plugin

```bash
# From a git repository
openclaw plugins install git:github.com/hs-creative/openclaw-persistent-debounce@master

# Or from a local path during development
openclaw plugins install --link ./openclaw-persistent-debounce
```

### 2. Enable the plugin

```bash
openclaw plugins enable persistent-debounce
```

### 3. Configure OpenClaw

Add to your `~/.openclaw/openclaw.json`:

```json
{
  "plugins": {
    "entries": {
      "persistent-debounce": {
        "enabled": true,
        "config": {
          "dbPath": "~/.openclaw/persistent-debounce.db",
          "channels": {
            "whatsapp": {
              "enabled": true,
              "debounceMs": 60000,
              "accounts": ["lisa", "default"]
            }
          },
          "flushIntervalMs": 5000,
          "maxPendingAgeMs": 3600000
        }
      }
    }
  }
}
```

### 4. Disable the built-in debounce for the target channel

Set the channel's `debounceMs` to `0` in OpenClaw config so this plugin owns the delay:

```json
{
  "channels": {
    "whatsapp": {
      "accounts": {
        "lisa": {
          "debounceMs": 0
        }
      }
    }
  }
}
```

> **Important:** If you leave the built-in `debounceMs` active, messages will be delayed twice (once by core, once by this plugin). Always set `debounceMs: 0` on accounts managed by this plugin.

### 5. Restart the gateway

```bash
openclaw doctor
openclaw gateway restart
```

## Configuration Reference

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `dbPath` | `string` | `~/.openclaw/persistent-debounce.db` | SQLite database file path |
| `channels` | `object` | `{}` | Per-channel debounce configuration |
| `channels.<name>.enabled` | `boolean` | `true` | Enable debounce for this channel |
| `channels.<name>.debounceMs` | `number` | `60000` | Delay before flushing messages (ms) |
| `channels.<name>.accounts` | `string[]` | `[]` | Account IDs to manage. Empty = all accounts |
| `flushIntervalMs` | `number` | `5000` | How often to check for messages ready to flush |
| `maxPendingAgeMs` | `number` | `3600000` | Max age for pending messages before auto-recovery or discard |

## How it works

### Inbound flow

1. WhatsApp delivers a message to OpenClaw
2. The `message_received` hook intercepts it
3. The plugin writes the message to SQLite with status `pending`
4. The hook returns `cancel: true` so OpenClaw does **not** process it immediately
5. A background timer checks every `flushIntervalMs` for messages whose `scheduled_flush_at` has passed
6. Ready messages are grouped by conversation and flushed via `runEmbeddedAgent`
7. After a successful reply, messages are marked `answered`

### Deduplication

Each message gets a `dedupe_key` computed from:
- channel + account + sender + conversation + text + 1-second time bucket

If the same message is redelivered by the platform (e.g. after a crash), the UNIQUE constraint on `dedupe_key` rejects the insert. The duplicate is silently dropped with `cancel: true`, preventing a double reply.

### Crash recovery

On `gateway_start`, the plugin scans for messages with status `pending` or `flushing` that are within `maxPendingAgeMs`. These are normal timer checks — no special action is needed because the flush timer will pick them up on the next interval. If messages are older than the max age, they are considered stale and left for manual inspection.

### Answered tracking

When OpenClaw sends an outbound reply (`message_sending` hook), the plugin detects the session key and marks the corresponding queued messages as `answered`. This prevents re-flushing after a successful reply.

## Database schema

```sql
CREATE TABLE inbound_queue (
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
```

## Tool: `debounce_queue_status`

Agents can check queue status:

```json
{
  "tool": "debounce_queue_status",
  "params": {
    "channel": "whatsapp",
    "accountId": "lisa"
  }
}
```

## Security & Safety

- Messages are stored locally in SQLite only
- No external network calls
- The plugin does not read or modify message content — it only stores and forwards
- Deduplication prevents both accidental and malicious replay

## Development

```bash
git clone https://github.com/hs-creative/openclaw-persistent-debounce.git
cd openclaw-persistent-debounce
npm install
npm test

# Local link install
openclaw plugins install --link .
openclaw plugins enable persistent-debounce
openclaw gateway restart
```

## Requirements

- OpenClaw >= 2026.3.24-beta.2
- Node.js >= 22.19
- `better-sqlite3` (installed automatically)

## License

MIT
