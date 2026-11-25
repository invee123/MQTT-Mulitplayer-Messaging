import type { MqttWebSocketConstructor } from './signal.js'

export type RoomStatus = 'disconnected' | 'connecting' | 'connected' | 'error'
export type RoomRole = 'solo' | 'host' | 'guest' | 'display'

export type StorageLike = {
  getItem: (key: string) => string | null
  setItem: (key: string, value: string) => void
  removeItem?: (key: string) => void
}

export type DocumentLike = {
  readonly visibilityState: string
  addEventListener: (type: 'visibilitychange', listener: () => void) => void
  removeEventListener: (type: 'visibilitychange', listener: () => void) => void
}

export type WindowLike = {
  addEventListener: (type: 'online' | 'pageshow', listener: () => void) => void
  removeEventListener: (type: 'online' | 'pageshow', listener: () => void) => void
}

export type RoomError = {
  code: string
  message: string
  intentId?: string
}

export type TvAssignOutcome = {
  status: 'acknowledged' | 'optimistic' | 'timeout'
  tvCode: string
  roomCode: string
}

export type HostHandoffPayload<TState = unknown, TChat = unknown> = {
  state: TState
  peerIds: string[]
  displayPeerIds: string[]
  peerNames?: Record<string, string>
  chatHistory?: TChat[]
  meta?: Record<string, unknown>
}

/** FIFO-bounded set for deduplicating control messages and intents across brokers. */
export class BoundedIdSet {
  private readonly set = new Set<string>()
  private readonly queue: string[] = []
  private readonly cap: number

  constructor(cap = 500) {
    this.cap = cap
  }

  has(id: string): boolean {
    return this.set.has(id)
  }

  add(id: string): boolean {
    if (this.set.has(id)) return false
    this.set.add(id)
    this.queue.push(id)
    while (this.queue.length > this.cap) {
      const oldest = this.queue.shift()
      if (oldest !== undefined) {
        this.set.delete(oldest)
      }
    }
    return true
  }
}

export type RoomInfo = {
  role: RoomRole
  code: string
  status: RoomStatus
  clientId: string
  peers: number
  error: string
  /**
   * Set when the host has missed 3+ HELLO heartbeats (~12 s). The room stays
   * open — guests show a non-blocking standby banner instead of erroring out.
   */
  hostDown?: boolean
}

/** Public-safe baseline snapshot replayed to late joiners / freshly-subscribed guests. */
export type PublicStateSnapshot<TState = unknown> = {
  type: 'PUBLIC_STATE'
  state: TState
  updatedAt: number
  hostEpoch: number
}

/** Stale baselines older than this are ignored so ancient rooms do not resurrect. */
export const PUBLIC_STATE_TTL_MS = 6 * 60 * 60 * 1000

export function isExpiredPublicSnapshot(
  snapshot: Partial<PublicStateSnapshot<unknown>>,
  now = Date.now(),
  ttlMs = PUBLIC_STATE_TTL_MS,
): boolean {
  if (typeof snapshot?.updatedAt !== 'number' || !Number.isFinite(snapshot.updatedAt)) return true
  return now - snapshot.updatedAt > ttlMs
}

export type RoomCallbacks<TState = unknown, TIntent = unknown, TChat = unknown> = {
  onStatus: (info: RoomInfo) => void
  /** Guest/display receives authoritative snapshots from host. */
  onSnapshot?: (state: TState) => void
  /** Host receives intents from connected guests. */
  onIntent?: (intent: TIntent, senderId: string) => void
  /** Host is notified when a peer joins or leaves so it can sync state or update its roster. */
  onPeerChange?: (peerIds: string[], leftId?: string, peerNames?: Record<string, string>) => void
  /** Guest provides its current player name so JOIN/PING can carry it immediately. */
  getPlayerName?: () => string
  /** Guest receives an individual live chat message. */
  onChat?: (message: TChat) => void
  /** Guest receives full chat history sync from host. */
  onChatSync?: (messages: TChat[]) => void
  /** A peer started or stopped typing in the chat composer. Transient UI signal, no state change. */
  onTyping?: (peerId: string, typing: boolean) => void
  /** Guest/display receives targeted errors from the host (e.g. rejected intents). */
  onError?: (error: RoomError) => void
  /** Guest receives a session token / playerId binding from the host. */
  onSessionToken?: (playerId: string, playerToken: string) => void
  /** Guest receives a live host-handoff payload inviting it to assume host authority. */
  onHostHandoff?: (payload: HostHandoffPayload<TState, TChat>, fromHostPeerId: string) => void
  /** Guest is notified when HELLO announces a new host peerId or hostSessionId. */
  onHostChanged?: (hostPeerId: string, hostSessionId?: string) => void
}

/**
 * Transport-agnostic star-topology room contract.
 * Implemented by MQTT rooms in this library; can also be swapped for
 * WebSockets / Durable Objects / Firebase without changing UI components.
 */
export type RoomTransport<TState = unknown, TIntent = unknown, TSecret = void, TChat = unknown> = {
  broadcast: (state: TState, secret?: TSecret) => void
  broadcastChat: (message: TChat) => void
  sendIntent: (intent: TIntent) => void
  /** Announce that the local player started/stopped typing. Fire-and-forget, relayed via the lobby topic. */
  sendTyping: (typing: boolean) => void
  kickPeer: (peerId: string) => void
  close: () => void
  /** Send a targeted error to a peer (host-only). */
  sendError?: (peerId: string, error: RoomError) => void
  /** Send a session token assignment to a peer (host-only). */
  sendSession?: (peerId: string, playerId: string, playerToken: string) => void
  /** Initiate a live host handoff to another peer with retry + ACK (host-only). */
  initiateHostHandoff?: (
    targetPeerId: string,
    payload: HostHandoffPayload<TState, TChat>,
    options?: { retryMs?: number; timeoutMs?: number },
  ) => Promise<boolean>
  /** Force a wake/re-sync check on sockets and state. */
  requestSync?: () => void
}

export type MqttRoomTimingOptions = {
  /** Overrides the host HELLO cadence (testing). */
  helloIntervalMs?: number
  /** Overrides how long the guest tolerates a missing HELLO before standby (testing). */
  hostDownAfterMs?: number
  /** Overrides how often the guest checks HELLO freshness (testing). */
  hostCheckIntervalMs?: number
  /** Overrides the guest reconnect delay after its sockets drop (testing). */
  guestReconnectDelayMs?: number
  /** Overrides the initial guest connection timeout (ms). */
  connectTimeoutMs?: number
  /** Overrides the host stale-peer sweep threshold (ms). */
  peerStaleMs?: number
  /** Overrides the host stale-peer sweep interval (ms). */
  peerSweepIntervalMs?: number
  /** Overrides the guest PING cadence (ms). */
  peerPingMs?: number
  /** Overrides the guest JOIN retry cadence (ms). */
  joinRetryMs?: number
}

export type MqttRoomConfig = {
  /** MQTT topic prefix for rooms. Defaults to `'mmp/v1/room'`. */
  topicPrefix?: string
  /** Prefix for host/guest peer IDs. Defaults to `'mmp-'`. */
  peerPrefix?: string
  /** Prefix for read-only display peer IDs. Defaults to `'mmp-d-'`. */
  displayPeerPrefix?: string
  /** List of WSS MQTT broker URLs to connect to simultaneously. */
  brokers?: readonly string[]
  /** Room code character length. Defaults to 4. */
  codeLength?: number
  /** Optional timing overrides (mainly for tests). */
  timing?: MqttRoomTimingOptions
  /** Optional WebSocket constructor override (mainly for tests). */
  WebSocketImpl?: MqttWebSocketConstructor
  /** Optional document lifecycle source for wake detection (testing/SSR). */
  document?: DocumentLike | null
  /** Optional window lifecycle source for wake detection (testing/SSR). */
  window?: WindowLike | null
  /** Optional clock source (mainly for tests). */
  now?: () => number
  /** Maximum length for player names carried in JOIN/PING. Defaults to 18. */
  maxPlayerNameLength?: number
  /** Initial per-broker reconnect backoff in ms. Defaults to 1500. */
  reconnectBackoffMs?: number
  /** Maximum per-broker reconnect backoff in ms. Defaults to 30000. */
  maxReconnectBackoffMs?: number
  /** Threshold of inactivity before wakeAndResync recycles sockets. Defaults to 12000. */
  wakeReconnectMs?: number
}

export type MqttHostOptions<TState, TIntent, TSecret = void, TChat = unknown> = MqttRoomConfig & {
  /** Pre-selected room code; if omitted, a random code is generated. */
  initialCode?: string
  /** Filters/redacts `state` for a specific peer before sending a direct `STATE` snapshot. */
  sanitizeForPeer?: (
    state: TState,
    secret: TSecret | undefined,
    peerId: string,
    isDisplay: boolean,
  ) => TState
  /** Filters/redacts `state` before publishing the retained `PUBLIC_STATE` baseline on the lobby topic. */
  sanitizeForBaseline?: (state: TState, secret: TSecret | undefined) => TState
  /** Custom chat history merger/capper when `broadcastChat` is called. */
  upsertChat?: (history: readonly TChat[], message: TChat) => TChat[]
  /** Converts a `JOIN`/`PING` player name change into a host `onIntent` call. */
  createJoinIntent?: (name: string) => TIntent
  /** Identifies a chat-history sync request intent from a guest/display. */
  isChatSyncIntent?: (intent: TIntent) => boolean
  /** Optional host session ID included in HELLO so guests detect host restarts. */
  hostSessionId?: string
  /** Optional explicit host peer ID (e.g. after assuming host via handoff). */
  peerId?: string
  /** Additional fallback host peer ID to subscribe to (e.g. default deterministic host topic). */
  fallbackHostId?: string
  /** Optional hook invoked on JOIN with full metadata (name, role, playerToken). */
  onJoin?: (peerId: string, info: { role: 'player' | 'display'; name?: string; playerToken?: string }) => void
  /** When true, skips CHAT and CHAT_SYNC delivery to read-only display peers. */
  isolateDisplayChat?: boolean
  /** Initial chat history when resuming or taking over as host. */
  initialChatHistory?: readonly TChat[]
  /** Initial state when resuming from a checkpoint or host handoff. */
  initialState?: TState
  /** Initial secret when resuming or taking over as host. */
  initialSecret?: TSecret
  /** Initial guest peer IDs to pre-populate on resume/handoff. */
  initialPeerIds?: readonly string[]
  /** Initial display peer IDs to pre-populate on resume/handoff. */
  initialDisplayPeerIds?: readonly string[]
  /** Initial peer display names to pre-populate on resume/handoff. */
  initialPeerNames?: Record<string, string>
}

export type MqttSubscriberOptions<TState, TIntent = unknown, TChat = unknown> = MqttRoomConfig & {
  /** Redacts a retained `PUBLIC_STATE` baseline on receipt so it never shows private data. */
  sanitizeBaselineOnReceive?: (state: TState) => TState
  /** Runtime validator for incoming `CHAT` / `CHAT_SYNC` payloads. */
  validateChat?: (value: unknown) => value is TChat
  /** Identifies intents that a read-only display peer is still allowed to send (e.g. `'chat-sync'`). */
  isDisplayAllowedIntent?: (intent: TIntent) => boolean
  /** Provides a session token to include on JOIN/PING/INTENT/LEAVE. */
  getPlayerToken?: () => string | null | undefined
  /** Extracts a monotonic version number from snapshots to drop stale out-of-order deliveries. */
  getStateVersion?: (state: TState) => number
  /** When true, skips CHAT, CHAT_SYNC, and TYPING callbacks on display peers. */
  isolateDisplayChat?: boolean
  /** Optional explicit subscriber peer ID. */
  peerId?: string
}
