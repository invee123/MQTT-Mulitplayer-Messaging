# MQTT-Mulitplayer-Messaging

Zero-dependency multiplayer room transport over MQTT-over-WSS for browser games.

Designed for casual, turn-based, party, tabletop, and card web games where players join from phones over cellular/carrier NATs without needing a TURN server or custom backend:

- **Zero runtime dependencies** — minimal binary MQTT 3.1.1 client over browser `WebSocket` (`ArrayBuffer`, `ArrayBufferView`, and `Blob` frames, `retain` support, dynamic `subscribe`, `DISCONNECT`, and `WebSocketImpl` injection).
- **Multi-broker fan-out & resilient reconnect** — connects to multiple WSS MQTT brokers simultaneously (`VITE_MQTT_BROKERS` or `brokers` override supported), heals individual dropped brokers with exponential backoff, and deduplicates intents/control packets via `BoundedIdSet`.
- **Two room topologies** — **Star** (authoritative host + thin guests + optional TV display, with per-peer secret redaction) and **Hostless Peer-Replicated** (deterministic CRDT state merging).
- **Mobile screen-lock & wake recovery** — automatic `visibilitychange` / `online` / `pageshow` socket recycle & re-sync (`requestSync()`), plus `createWakeLockController` for the Screen Wake Lock API.
- **Read-only Display / TV mode + 4-char pairing** — `createMqttDisplayRoom`, `createMqttTvListener`, `assignRoomToTvDisplay`, and `assignRoomToTvDisplayDetailed`.

---

## Choosing a Topology & Limitations

Pick the topology that matches your game's trust and state requirements:

| Feature | Star Topology (`createMqttHostRoom` / `Guest` / `Display`) | Hostless Peer Room (`createMqttPeerRoom`) |
| --- | --- | --- |
| **Authority** | Single authoritative host browser processes `onIntent` | Every peer holds state and merges via `mergeState` |
| **Hidden info / secrets** | **Yes** (`sanitizeForPeer`, `sanitizeForBaseline`) — e.g. card hands, secret roles | **No** — every peer sees the full shared state |
| **If creator locks/leaves** | `hostDown` standby banner; optional `initiateHostHandoff` | Seamless — remaining peers keep playing |
| **Chat, typing, kick, session tokens** | Built-in | Not included |
| **Read-only TV / Display mode** | Built-in (`createMqttDisplayRoom`) | Not included |
| **Best for** | Card games (Cribbage, Poker), social deduction, Jackbox-style TV games | Shared boards, scorekeepers, co-op puzzles with no secrets |

### Design Constraints to Know Before Building

- **Turn-based & casual pacing (QoS 0 JSON):** Messages are JSON-serialized and sent with MQTT QoS 0. `broadcast()` and `publishState()` send full state snapshots (retained `PUBLIC_STATE` baselines are throttled to 150 ms). This is not suitable for 30–60 Hz twitch action games, FPS/physics sync, or binary delta compression.
- **Casual trust model (no server-side anti-cheat):** Public MQTT brokers have no topic ACLs or user authentication in this client. Anyone with the topic prefix and room code can subscribe or publish to the room, and in Star mode the host's browser memory holds `TSecret`.
- **No matchmaking or database:** Rooms are joined out-of-band via 4-character codes or URLs. Persistent user accounts, leaderboards, and global lobbies are out of scope.
- **Public broker limits:** `DEFAULT_SIGNAL_BROKERS` uses free public endpoints (`wss://broker.emqx.io:8084/mqtt` and `wss://broker.hivemq.com:8884/mqtt`). For production scale or custom SLAs, supply your own WSS MQTT broker(s) via `brokers` or `VITE_MQTT_BROKERS`.

---

## Installation

From GitHub or a local sibling checkout:

```bash
npm install mqtt-multiplayer-messaging
# or local path:
npm install ../MQTT-Mulitplayer-Messaging
```

Ships ESM JavaScript + TypeScript declarations (`dist/index.js`, `dist/index.d.ts`).

---

## Quick Start

### 1. Star-Topology Room (Host + Guests + TV Display)

Use Star topology whenever you need an authoritative host, hidden per-player state (like cards in hand), chat, session rebinding, or a shared TV screen.

```ts
import {
  createMqttHostRoom,
  createMqttGuestRoom,
  type RoomInfo,
  type RoomTransport,
} from 'mqtt-multiplayer-messaging'

type GameState = {
  version: number
  round: number
  phase: 'guessing' | 'revealed'
  target: number | null
  guesses: Record<string, number>
}

type GameIntent =
  | { type: 'join'; name: string }
  | { type: 'guess'; value: number }
  | { type: 'reveal' }

const ROOM_CONFIG = {
  topicPrefix: 'mygame/v1/room',
  peerPrefix: 'mygame-',
  displayPeerPrefix: 'mygame-d-',
}
```

#### Host a room

```ts
let state: GameState = { version: 1, round: 1, phase: 'guessing', target: null, guesses: {} }
let secretTarget = 120

const host: RoomTransport<GameState, GameIntent, number> = createMqttHostRoom(
  {
    onStatus(info: RoomInfo) {
      console.log('Room code:', info.code, 'Status:', info.status, 'Peers:', info.peers)
    },
    onPeerChange(peerIds, leftId, peerNames) {
      host.broadcast(state, secretTarget)
    },
    onIntent(intent, senderId) {
      if (intent.type === 'guess') {
        state = {
          ...state,
          version: state.version + 1,
          guesses: { ...state.guesses, [senderId]: intent.value },
        }
      } else if (intent.type === 'reveal') {
        state = { ...state, version: state.version + 1, phase: 'revealed', target: secretTarget }
      }
      host.broadcast(state, secretTarget)
    },
  },
  {
    ...ROOM_CONFIG,
    // Redact private data per peer (guests vs. shared display)
    sanitizeForPeer(s, secret, peerId, isDisplay) {
      if (s.phase === 'revealed') return { ...s, target: secret ?? null }
      return {
        ...s,
        target: null,
        guesses: isDisplay ? s.guesses : peerId in s.guesses ? { [peerId]: s.guesses[peerId] } : {},
      }
    },
    // Redact private data in the retained PUBLIC_STATE baseline for late joiners
    sanitizeForBaseline(s, secret) {
      return {
        ...s,
        target: s.phase === 'revealed' ? (secret ?? null) : null,
        guesses: {},
      }
    },
    createJoinIntent: (name) => ({ type: 'join', name }),
  },
)
```

#### Join as a guest

```ts
const guest = createMqttGuestRoom<GameState, GameIntent>(
  'K7MP',
  {
    onStatus(info) {
      if (info.hostDown) {
        // Host missed 3+ HELLO heartbeats (~12s) — show non-blocking standby banner
      }
    },
    onSnapshot(nextState) {
      state = nextState
    },
    onError(err) {
      console.warn(err.code, err.message)
    },
    getPlayerName: () => 'Alice',
  },
  {
    ...ROOM_CONFIG,
    // Extra safety: redact baseline on receipt before host sends a personalized STATE
    sanitizeBaselineOnReceive: (s) => ({
      ...s,
      target: s.phase === 'revealed' ? s.target : null,
      guesses: {},
    }),
    // Drop stale out-of-order snapshots across multi-broker delivery
    getStateVersion: (s) => s.version,
  },
)

guest.sendIntent({ type: 'guess', value: 85 })
```

#### Reconnect Sessions, Errors, Chat, Kick & Host Handoff

- **Session tokens (re-attaching refreshed players):**
  - Host: call `host.sendSession?.(peerId, playerId, playerToken)` after `onJoin(peerId, info)` (or `onIntent`).
  - Guest: handle `onSessionToken(playerId, playerToken)` to store the token, and pass `getPlayerToken: () => storedToken` in `MqttSubscriberOptions` so future `JOIN`/`PING`/`INTENT`/`LEAVE` messages include it.
- **Targeted errors & moderation:**
  - Host: `host.sendError?.(peerId, { code: 'INVALID_MOVE', message: 'Not your turn' })` and `host.kickPeer(peerId)`.
  - Guest: handle `onError(error)`.
- **Room chat & typing indicators:**
  - Host & Guests: call `room.broadcastChat(message)` and `room.sendTyping(true | false)`.
  - Listeners: handle `onChat(message)`, `onChatSync(messages)`, and `onTyping(peerId, typing)`.
  - Configure `upsertChat` / `initialChatHistory` / `isolateDisplayChat` on the host, and `validateChat` / `isDisplayAllowedIntent` on guests/displays.
- **Live host handoff:**
  - Host: `const ok = await host.initiateHostHandoff?.(targetPeerId, { state, peerIds, displayPeerIds, peerNames, chatHistory, meta })`.
  - Target guest: handle `onHostHandoff(payload, fromHostPeerId)`, close the guest room, and start `createMqttHostRoom` with `initialCode`, `initialState`, `initialPeerIds`, `initialDisplayPeerIds`, `initialPeerNames`, `initialChatHistory`, and `fallbackHostId`.
  - Other guests: notified via `onHostChanged(hostPeerId, hostSessionId)`.

---

### 2. Hostless Peer-Replicated Room (`createMqttPeerRoom`)

For games without hidden secrets where any peer can act even when the creator locks their phone:

```ts
import { createMqttPeerRoom } from 'mqtt-multiplayer-messaging'

const peer = createMqttPeerRoom({
  roomCode: 'K7MP',
  topicPrefix: 'cranium/v1/room',
  peerPrefix: 'cr-',
  createEmptyState: (code) => ({ roomCode: code, seq: 0, updatedAt: 0 }),
  mergeState: (local, incoming) => {
    const winner = incoming.seq > local.seq ? incoming : local
    return {
      state: winner,
      changed: winner !== local,
      incomingWasBehind: incoming.seq < local.seq,
    }
  },
  hasState: (s) => s.updatedAt > 0,
  storageKey: 'mygame-room-K7MP', // optional localStorage persistence
  onState: (s) => console.log('Synced state:', s),
  onStatus: (status) => console.log('Peer status:', status), // 'connecting' | 'connected' | 'reconnecting' | 'error'
  onPeerCount: (count) => console.log('Peers in room:', count),
  onError: (msg) => console.warn(msg),
})

// Publish a new state (merges locally + broadcasts retained snapshot):
peer.publishState({ ...peer.getState(), seq: peer.getState().seq + 1, updatedAt: Date.now() })

// Force a wake/re-sync check:
peer.requestSync()

// Leave cleanly (pass clearIfLast: true to delete retained state if you're the last peer):
peer.close({ clearIfLast: true })
```

---

### 3. Read-Only TV / Display Mode & 4-Char Pairing

```ts
import {
  createMqttDisplayRoom,
  createMqttTvListener,
  assignRoomToTvDisplay,
  assignRoomToTvDisplayDetailed,
  generateRoomCode,
} from 'mqtt-multiplayer-messaging'

// On the TV / shared screen:
const tvCode = generateRoomCode()
const listener = createMqttTvListener(
  tvCode,
  {
    onAssignRoom(roomCode) {
      listener.close()
      createMqttDisplayRoom(
        roomCode,
        {
          onStatus: (info) => console.log('TV status:', info.status),
          onSnapshot: (s) => console.log('TV snapshot:', s),
        },
        ROOM_CONFIG,
      )
    },
  },
  ROOM_CONFIG,
)

// On a player's phone (e.g. after scanning a QR code with ?tv=XXXX):
const ok = await assignRoomToTvDisplay(tvCode, 'K7MP', ROOM_CONFIG)
// Or get detailed delivery status:
const outcome = await assignRoomToTvDisplayDetailed(tvCode, 'K7MP', ROOM_CONFIG)
// outcome: { status: 'acknowledged' | 'optimistic' | 'timeout', tvCode, roomCode }
```

---

### 4. Screen Wake Lock (`createWakeLockController`)

Keeps mobile screens awake during a game and automatically re-acquires the lock when returning to the tab (`visibilitychange` → `'visible'`):

```ts
import { createWakeLockController } from 'mqtt-multiplayer-messaging'

const wakeLock = createWakeLockController({
  onStatusChange: (s) => console.log('Wake lock:', s.status), // 'idle' | 'active' | 'unsupported' | 'denied' | 'released'
})

await wakeLock.acquire()
const unsubscribe = wakeLock.subscribe((s) => console.log(s.active))
// Later:
await wakeLock.release()
unsubscribe()
wakeLock.destroy()
```

---

## Configuration Reference

### Shared Room Config (`MqttRoomConfig`)

Accepted by `createMqttHostRoom`, `createMqttGuestRoom`, `createMqttDisplayRoom`, `createMqttPeerRoom`, `createMqttTvListener`, and `assignRoomToTvDisplay*`:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `topicPrefix` | `string` | `'mmp/v1/room'` | Base MQTT topic prefix (always namespace per game/version) |
| `peerPrefix` | `string` | `'mmp-'` | Client ID prefix for host and guest peers |
| `displayPeerPrefix` | `string` | `'mmp-d-'` | Client ID prefix for read-only display peers |
| `brokers` | `readonly string[]` | `resolveDefaultBrokers()` | List of `wss://` MQTT broker URLs to connect to simultaneously |
| `codeLength` | `number` | `4` | Room code character length |
| `timing` | `MqttRoomTimingOptions` | See below | Heartbeat, timeout, and sweep timing overrides |
| `WebSocketImpl` | `MqttWebSocketConstructor` | `globalThis.WebSocket` | Custom WebSocket constructor (Node/SSR/tests) |
| `document` | `DocumentLike \| null` | `globalThis.document` | Visibility listener source (`null` disables) |
| `window` | `WindowLike \| null` | `globalThis.window` | `online`/`pageshow` listener source (`null` disables) |
| `now` | `() => number` | `() => Date.now()` | Clock source (useful for deterministic tests) |
| `maxPlayerNameLength` | `number` | `18` | Max characters for player names in `JOIN`/`PING` |
| `reconnectBackoffMs` | `number` | `1500` | Initial per-broker reconnect backoff (ms) |
| `maxReconnectBackoffMs` | `number` | `30000` | Maximum per-broker reconnect backoff (ms) |
| `wakeReconnectMs` | `number` | `12000` | Inactivity threshold before wake recycles sockets (ms) |

> **Environment variable override:** If `import.meta.env.VITE_MQTT_BROKERS` is set to a comma-separated list of `wss://` URLs, `resolveDefaultBrokers()` uses it instead of `DEFAULT_SIGNAL_BROKERS`.

### Timing Overrides (`MqttRoomTimingOptions`)

| Option | Star-Room Default | Description |
| --- | --- | --- |
| `helloIntervalMs` | `4000` | Host `HELLO` heartbeat cadence |
| `hostDownAfterMs` | `12000` | Guest tolerance for missing `HELLO` before setting `hostDown: true` |
| `hostCheckIntervalMs` | `2000` | How often guests check `HELLO` freshness |
| `guestReconnectDelayMs` | `2000` | Guest reconnect delay after all broker sockets drop |
| `connectTimeoutMs` | `15000` | Initial connection timeout |
| `peerStaleMs` | `20000` | Host stale-peer sweep threshold |
| `peerSweepIntervalMs` | `5000` | Host stale-peer sweep interval |
| `peerPingMs` | `6000` | Guest `PING` heartbeat cadence |
| `joinRetryMs` | `1500` | Guest `JOIN` retry cadence until host responds |

### Host Options (`MqttHostOptions<TState, TIntent, TSecret, TChat>`)

Extends `MqttRoomConfig`:

- `initialCode?: string` — Use a pre-selected room code instead of generating one.
- `sanitizeForPeer?(state, secret, peerId, isDisplay): TState` — Filter/redact state per recipient before sending `STATE`.
- `sanitizeForBaseline?(state, secret): TState` — Filter/redact state before publishing the retained `PUBLIC_STATE` baseline.
- `upsertChat?(history, message): TChat[]` — Custom chat history merger/capper (default caps at `60` messages and deduplicates by `message.id`).
- `createJoinIntent?(name): TIntent` — Convert a `JOIN`/`PING` player name change into an `onIntent` call.
- `isChatSyncIntent?(intent): boolean` — Identify chat-history sync requests (default checks `intent.type === 'chat-sync'`).
- `onJoin?(peerId, { role, name?, playerToken? })` — Hook invoked when a peer joins.
- `isolateDisplayChat?: boolean` — Skip `CHAT` / `CHAT_SYNC` delivery to display peers.
- `hostSessionId?: string`, `peerId?: string`, `fallbackHostId?: string` — Explicit host identity and fallback subscription for restarts/handoffs.
- `initialState?`, `initialSecret?`, `initialChatHistory?`, `initialPeerIds?`, `initialDisplayPeerIds?`, `initialPeerNames?` — Pre-populate host state when resuming or taking over via handoff.

### Guest / Display Options (`MqttSubscriberOptions<TState, TIntent, TChat>`)

Extends `MqttRoomConfig`:

- `sanitizeBaselineOnReceive?(state): TState` — Redact a retained `PUBLIC_STATE` baseline on receipt.
- `validateChat?(value): value is TChat` — Runtime validator for incoming `CHAT` / `CHAT_SYNC` payloads.
- `isDisplayAllowedIntent?(intent): boolean` — Allow specific intents from read-only display peers (e.g. `'chat-sync'`).
- `getPlayerToken?(): string | null | undefined` — Attach a session token to `JOIN`, `PING`, `INTENT`, and `LEAVE`.
- `getStateVersion?(state): number` — Monotonic version extractor to drop stale out-of-order snapshots.
- `isolateDisplayChat?: boolean` — Skip `CHAT`, `CHAT_SYNC`, and `TYPING` callbacks on display peers.
- `peerId?: string` — Explicit subscriber peer ID.

### Peer Room Options (`MqttPeerRoomOptions<TState>`)

Extends `MqttRoomConfig`:

- **Required:** `roomCode`, `createEmptyState(roomCode)`, `mergeState(local, incoming) => { state, changed, incomingWasBehind }`, `onState(state)`, `onStatus(status)`, `onPeerCount(count)`.
- **Optional:** `clientId`, `initialState`, `publishInitialState`, `validateState(value, roomCode)`, `isExpiredState(state, now, ttlMs)`, `hasState(state)`, `storageKey`, `storage` (`StorageLike | null`), `rng`, `heartbeatMs` (default `8000`), `peerStaleMs` (default `25000`), `peerSweepMs` (default `5000`), `roomTtlMs` (default `PUBLIC_STATE_TTL_MS`, 6 hours), `onError(message)`.

---

## Room Codes, URLs & Utilities

- **Codes & Tokens:**
  - `CODE_ALPHABET` (`'ABCDEFGHJKMNPQRSTUVWXYZ23456789'`, omits `0/O` and `1/I/L`), `ROOM_CODE_LENGTH` (`4`).
  - `generateRoomCode(length = 4)`, `normalizeRoomCode(raw, length = 4)`, `isValidRoomCode(code, length = 4)`, `randomToken(length = 12)`.
  - `parseJoinCode(raw, length = 4)` — extracts codes from raw strings, `?room=`, `?tv=`, `?pair=`, `/tv/CODE`, or `/display/CODE`.
- **URL Helpers:**
  - `roomUrlFor(code, baseHref?, codeLength?)`, `displayUrlFor(code?, baseHref?, codeLength?)`, `tvPairUrlFor(tvCode, baseHref?, codeLength?)`.
  - `readDisplayModeFromUrl(search?, pathname?)`, `readRoomFromUrl(search?, pathname?, codeLength?)`, `readTvPairFromUrl(search?, codeLength?)`, `writeRoomToUrl(code, options?)`.
- **Other Helpers:**
  - `BoundedIdSet(cap = 500)` — FIFO-bounded deduplication set.
  - `isDisplayPeerId(peerId, displayPeerPrefix?)` — checks if a peer ID belongs to a display peer.
  - `isExpiredPublicSnapshot(snapshot, now?, ttlMs?)` — checks against `PUBLIC_STATE_TTL_MS` (6 hours).
  - `roomStateTopic(roomCode, topicPrefix?, codeLength?)`, `roomSyncTopic(roomCode, topicPrefix?, codeLength?)` — topic builders for peer rooms.

---

## Low-Level MQTT Signal Client

If you want to build your own protocol on top of the raw MQTT-over-WSS client:

```ts
import {
  connectMqttSignal,
  brokersForRoom,
  resolveDefaultBrokers,
  DEFAULT_SIGNAL_BROKERS,
} from 'mqtt-multiplayer-messaging'

const client = connectMqttSignal({
  brokers: brokersForRoom('K7MP', resolveDefaultBrokers()),
  clientId: 'my-client-123',
  subscribeTopic: ['mygame/K7MP/lobby'],
  onReady: () => client.publish('mygame/K7MP/lobby', { hello: true }, false),
  onMessage: (topic, payload, meta) => console.log(topic, payload, meta?.retain),
  onDisconnect: (hadConnected) => console.log('Disconnected', hadConnected),
})
```

`MqttSignalClient` exposes `publish(topic, data, retain?)`, `publishRaw(topic, payload, retain?)`, `deleteRetained(topic)`, `subscribe(topic)`, `isReady()`, `isOpen()`, and `close()`.

---

## Development

```bash
npm install
npm run check   # TypeScript typecheck
npm run build   # Emit dist/ JS + .d.ts
npm test        # Run unit tests
```
