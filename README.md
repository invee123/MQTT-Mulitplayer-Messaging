# MQTT-Mulitplayer-Messaging

Zero-dependency multiplayer room transport over MQTT-over-WSS for browser games.

Designed for casual party and tabletop web games where players join from phones over cellular/carrier NATs without needing a TURN server or custom backend:

- **Zero runtime dependencies** — minimal binary MQTT 3.1.1 client over browser `WebSocket` (`ArrayBuffer`, `ArrayBufferView`, and `Blob` frames, `retain` support, dynamic `subscribe`, `DISCONNECT`, and `WebSocketImpl` injection).
- **Multi-broker fan-out & resilient reconnect** — connects to multiple public WSS MQTT brokers simultaneously (`VITE_MQTT_BROKERS` override supported), heals individual dropped brokers with exponential backoff, and deduplicates intents/control packets via `BoundedIdSet`.
- **Two room topologies:**
  - **Star topology (Authoritative Host + Thin Guests)** — `createMqttHostRoom`, `createMqttGuestRoom`, `createMqttDisplayRoom`: per-peer secret redaction (`sanitizeForPeer`, `sanitizeForBaseline`), retained `PUBLIC_STATE` baseline, `HELLO`/`PING` liveness, `hostDown` standby banner, targeted `sendError`/`onError`, `sendSession`/`onSessionToken`, and live `initiateHostHandoff` / `HOST_HANDOFF_ACK`.
  - **Hostless peer-replicated CRDT rooms** — `createMqttPeerRoom`: every peer holds shared state, publishes retained snapshots, merges concurrent updates deterministically (`mergeState`), suppresses `SYNC_REQ` storms, heals lagging peers, counts anonymous presence (`HERE`/`BYE`), and cleans up on `close({ clearIfLast: true })`.
- **Mobile screen-lock & wake recovery** — automatic `visibilitychange` / `online` / `pageshow` socket recycle & re-sync (`requestSync()`), plus `createWakeLockController` for the Screen Wake Lock API.
- **Read-only Display / TV mode + 4-char pairing** — `createMqttDisplayRoom`, `createMqttTvListener`, `assignRoomToTvDisplay`, and `assignRoomToTvDisplayDetailed`.

---

## Installation

From GitHub or a local sibling checkout:

```bash
npm install mqtt-multiplayer-messaging
# or local path:
npm install ../MQTT-Mulitplayer-Messaging
```

---

## Quick Start

### 1. Star-Topology Room (Host + Guests + TV Display)

```ts
import {
  createMqttHostRoom,
  createMqttGuestRoom,
  type RoomInfo,
  type RoomTransport,
} from 'mqtt-multiplayer-messaging'

type GameState = {
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
let state: GameState = { round: 1, phase: 'guessing', target: null, guesses: {} }
let secretTarget = 120

const host: RoomTransport<GameState, GameIntent, number> = createMqttHostRoom(
  {
    onStatus(info: RoomInfo) {
      console.log('Room code:', info.code, 'Status:', info.status, 'Peers:', info.peers)
    },
    onPeerChange() {
      host.broadcast(state, secretTarget)
    },
    onIntent(intent, senderId) {
      if (intent.type === 'guess') {
        state = { ...state, guesses: { ...state.guesses, [senderId]: intent.value } }
      } else if (intent.type === 'reveal') {
        state = { ...state, phase: 'revealed', target: secretTarget }
      }
      host.broadcast(state, secretTarget)
    },
  },
  {
    ...ROOM_CONFIG,
    sanitizeForPeer(s, secret, peerId, isDisplay) {
      if (s.phase === 'revealed') return { ...s, target: secret ?? null }
      return {
        ...s,
        target: null,
        guesses: isDisplay ? s.guesses : peerId in s.guesses ? { [peerId]: s.guesses[peerId] } : {},
      }
    },
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
        // Show non-blocking "Waiting for host..." banner
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
    sanitizeBaselineOnReceive: (s) => ({
      ...s,
      target: s.phase === 'revealed' ? s.target : null,
      guesses: {},
    }),
  },
)

guest.sendIntent({ type: 'guess', value: 85 })
```

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
  onState: (s) => console.log('Synced state:', s),
  onStatus: (status) => console.log('Peer status:', status),
  onPeerCount: (count) => console.log('Peers in room:', count),
})
```

### 3. Read-only TV / Display mode & 4-char pairing

```ts
import {
  createMqttDisplayRoom,
  createMqttTvListener,
  assignRoomToTvDisplay,
  assignRoomToTvDisplayDetailed,
  generateRoomCode,
} from 'mqtt-multiplayer-messaging'

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

const ok = await assignRoomToTvDisplay(tvCode, 'K7MP', ROOM_CONFIG)
const outcome = await assignRoomToTvDisplayDetailed(tvCode, 'K7MP', ROOM_CONFIG)
// outcome.status: 'acknowledged' | 'optimistic' | 'timeout'
```

### 4. Screen Wake Lock (`createWakeLockController`)

```ts
import { createWakeLockController } from 'mqtt-multiplayer-messaging'

const wakeLock = createWakeLockController()
await wakeLock.acquire()
// Automatically re-acquires on visibilitychange -> 'visible'
// Later:
wakeLock.destroy()
```

---

## API Reference

### Room Transports
- `createMqttHostRoom(callbacks, options?)` → `RoomTransport<TState, TIntent, TSecret, TChat>`
- `createMqttGuestRoom(roomCode, callbacks, options?)` → `RoomTransport<TState, TIntent, never, TChat>`
- `createMqttDisplayRoom(roomCode, callbacks, options?)` → `RoomTransport<TState, TIntent, never, TChat>`
- `createMqttPeerRoom(options)` → `MqttPeerRoom<TState>`
- `createMqttTvListener(tvCode, callbacks, config?)` → `{ close: () => void }`
- `assignRoomToTvDisplay(tvCode, roomCode, options?)` → `Promise<boolean>`
- `assignRoomToTvDisplayDetailed(tvCode, roomCode, options?)` → `Promise<TvAssignOutcome>`

### Room Codes, URLs & Utilities
- `generateRoomCode(length = 4)` — uses unambiguous alphabet `ABCDEFGHJKMNPQRSTUVWXYZ23456789` (no `0/O`, `1/I/L`).
- `normalizeRoomCode(raw, length = 4)`, `isValidRoomCode(code, length = 4)`
- `parseJoinCode(raw, length = 4)` — extracts codes from raw strings, `?room=`, `?tv=`, `?pair=`, `/tv/CODE`, or `/display/CODE`.
- `roomUrlFor(code, baseHref?)`, `displayUrlFor(code?, baseHref?)`, `tvPairUrlFor(tvCode, baseHref?)`
- `readDisplayModeFromUrl(search?, pathname?)`, `readRoomFromUrl(search?, pathname?)`, `readTvPairFromUrl(search?)`, `writeRoomToUrl(code, options?)`
- `createWakeLockController(options?)`
- `BoundedIdSet`

### Low-Level MQTT Signal Client
- `connectMqttSignal(options)` — raw MQTT 3.1.1 over WebSocket client (`CONNECT`, `SUBSCRIBE`, `PUBLISH`, `PINGREQ`, `DISCONNECT`, `deleteRetained`, `publishRaw`, dynamic `subscribe`).
- `brokersForRoom(code, brokers?)` — deterministic FNV-1a broker rotation hashed by room code.
- `resolveDefaultBrokers()` — reads `VITE_MQTT_BROKERS` if configured, falling back to `DEFAULT_SIGNAL_BROKERS`.

---

## Development

```bash
npm install
npm run check   # TypeScript typecheck
npm run build   # Emit dist/ JS + .d.ts
npm test        # Run unit tests
```
