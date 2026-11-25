import {
  DEFAULT_DISPLAY_PEER_PREFIX,
  DEFAULT_PEER_PREFIX,
  DEFAULT_SIGNAL_BROKERS,
  DEFAULT_TOPIC_PREFIX,
  attachWakeListeners,
  brokersForRoom,
  createMessageId,
  resolveConfig,
} from './brokers.js'
import { generateRoomCode, normalizeRoomCode, randomToken } from './codes.js'
import { connectMqttSignal, type MqttSignalClient } from './signal.js'
import {
  BoundedIdSet,
  isExpiredPublicSnapshot,
  type HostHandoffPayload,
  type MqttHostOptions,
  type MqttRoomConfig,
  type MqttSubscriberOptions,
  type PublicStateSnapshot,
  type RoomCallbacks,
  type RoomError,
  type RoomInfo,
  type RoomTransport,
  type TvAssignOutcome,
} from './types.js'

export {
  DEFAULT_DISPLAY_PEER_PREFIX,
  DEFAULT_PEER_PREFIX,
  DEFAULT_SIGNAL_BROKERS,
  DEFAULT_TOPIC_PREFIX,
  brokersForRoom,
}

const CONNECT_TIMEOUT_MS = 15_000
const JOIN_RETRY_MS = 1_500
const HOST_HELLO_MS = 4_000
const PEER_STALE_MS = 20_000
const PEER_SWEEP_INTERVAL_MS = 5_000
const PEER_PING_MS = 6_000
/** After this long without a host HELLO the guest shows standby (~3 missed heartbeats). */
const HOST_DOWN_AFTER_MS = 12_000
/** How often the guest checks HELLO freshness. */
const HOST_CHECK_INTERVAL_MS = 2_000
/** Minimum gap between retained baseline publishes when the host broadcasts. */
const BASELINE_THROTTLE_MS = 150
/** Delay before a guest retries after all its sockets drop. */
const GUEST_RECONNECT_DELAY_MS = 2_000
const DEFAULT_MAX_CHAT_HISTORY = 60

export function isDisplayPeerId(peerId: string, displayPeerPrefix = DEFAULT_DISPLAY_PEER_PREFIX): boolean {
  return peerId.startsWith(displayPeerPrefix)
}

type WireMessage<TState = unknown, TIntent = unknown, TChat = unknown> =
  | { type: 'HELLO'; src: string; hostSessionId?: string }
  | {
      type: 'JOIN'
      msgId?: string
      src: string
      role?: 'player' | 'display'
      name?: string
      playerToken?: string | null
    }
  | { type: 'LEAVE'; src: string; playerToken?: string | null }
  | { type: 'KICK'; msgId?: string; src: string }
  | { type: 'ERROR'; msgId?: string; src: string; error: RoomError }
  | { type: 'SESSION'; msgId?: string; src: string; playerId: string; playerToken: string }
  | { type: 'STATE'; state: TState }
  | { type: 'PUBLIC_STATE'; state: TState; updatedAt: number; hostEpoch: number }
  | { type: 'CHAT'; msgId?: string; message: TChat }
  | { type: 'CHAT_SYNC'; msgId?: string; messages: TChat[] }
  | { type: 'INTENT'; src: string; id?: string; playerToken?: string | null; intent: TIntent }
  | { type: 'TYPING'; src: string; typing: boolean }
  | { type: 'PING'; src: string; name?: string; playerToken?: string | null }
  | {
      type: 'HOST_HANDOFF'
      msgId: string
      src: string
      payload: HostHandoffPayload<TState, TChat>
    }
  | { type: 'HOST_HANDOFF_ACK'; msgId: string; src: string }
  | { type: 'TV_ASSIGN'; roomCode: string; src: string }
  | { type: 'TV_ACK'; roomCode: string; tvCode: string }

type PeerSlot = {
  peerId: string
  lastSeen: number
  role: 'player' | 'display'
  name?: string
}

function defaultChatUpsert<TChat>(history: readonly TChat[], message: TChat): TChat[] {
  const msgId =
    message && typeof message === 'object' && 'id' in message
      ? (message as { id?: unknown }).id
      : undefined
  if (typeof msgId === 'string' && msgId.length > 0) {
    const idx = history.findIndex(
      (item) => item && typeof item === 'object' && (item as { id?: unknown }).id === msgId,
    )
    if (idx >= 0) {
      const next = [...history]
      next[idx] = message
      return next.slice(-DEFAULT_MAX_CHAT_HISTORY)
    }
  }
  return [...history, message].slice(-DEFAULT_MAX_CHAT_HISTORY)
}

function defaultIsChatSyncIntent(intent: unknown): boolean {
  return (
    Boolean(intent) &&
    typeof intent === 'object' &&
    (intent as { type?: unknown }).type === 'chat-sync'
  )
}

type RoomSignalClient = MqttSignalClient & {
  reconnectAll: () => void
}

/**
 * Stay connected to every broker at once and publish to all of them.
 * Recovers individual dropped brokers with exponential backoff while another broker
 * remains alive, and emits `onDisconnect` only if all brokers drop.
 */
function connectRoomSignal(options: {
  code: string
  codeLength: number
  brokers: readonly string[]
  clientId: string
  subscribeTopics: readonly string[]
  WebSocketImpl?: MqttRoomConfig['WebSocketImpl']
  reconnectBackoffMs: number
  maxReconnectBackoffMs: number
  onReady: () => void
  onMessage: (topic: string, payload: string) => void
  onDisconnect: (hadConnected: boolean) => void
}): RoomSignalClient {
  const brokers = brokersForRoom(options.code, options.brokers, options.codeLength)
  const clients: Array<MqttSignalClient | null> = brokers.map(() => null)
  const backoffMs: number[] = brokers.map(() => options.reconnectBackoffMs)
  const reconnectTimers: Array<ReturnType<typeof globalThis.setTimeout> | null> = brokers.map(() => null)
  const extraTopics = new Set<string>()
  let readyCount = 0
  let activeCount = 0
  let announcedReady = false
  let closed = false
  let everReady = false
  let connAttempt = 0

  const clearTimer = (idx: number) => {
    const t = reconnectTimers[idx]
    if (t !== null && t !== undefined) {
      globalThis.clearTimeout(t)
      reconnectTimers[idx] = null
    }
  }

  const allTopics = () => {
    const combined = new Set<string>(options.subscribeTopics)
    for (const t of extraTopics) combined.add(t)
    return Array.from(combined)
  }

  const spawnBroker = (i: number) => {
    if (closed) return
    const broker = brokers[i]
    if (!broker) return
    clearTimer(i)
    const prev = clients[i]
    if (prev) {
      clients[i] = null
      prev.close()
    }
    connAttempt += 1
    activeCount += 1

    const client = connectMqttSignal({
      brokers: [broker],
      clientId: `${options.clientId}-b${i}-${connAttempt.toString(36)}`,
      subscribeTopic: allTopics(),
      WebSocketImpl: options.WebSocketImpl,
      onReady() {
        if (closed) return
        readyCount += 1
        everReady = true
        backoffMs[i] = options.reconnectBackoffMs
        if (!announcedReady) {
          announcedReady = true
          options.onReady()
        }
      },
      onMessage: options.onMessage,
      onDisconnect(hadConnected) {
        if (closed) return
        if (clients[i] === client) {
          clients[i] = null
        }
        activeCount = Math.max(0, activeCount - 1)
        if (hadConnected) readyCount = Math.max(0, readyCount - 1)

        if (activeCount === 0 && readyCount === 0) {
          // All brokers are down — hand off to room-level reconnect / hostDown handling.
          options.onDisconnect(everReady)
          return
        }

        // Single-broker drop while another broker is healthy/connecting: heal silently.
        const delay = backoffMs[i] ?? options.reconnectBackoffMs
        backoffMs[i] = Math.min(options.maxReconnectBackoffMs, delay * 2)
        reconnectTimers[i] = globalThis.setTimeout(() => {
          reconnectTimers[i] = null
          spawnBroker(i)
        }, delay)
      },
    })

    clients[i] = client
  }

  for (let i = 0; i < brokers.length; i += 1) {
    spawnBroker(i)
  }

  return {
    publish(topic, data, retain = false) {
      let sent = false
      for (const client of clients) {
        if (client?.publish(topic, data, retain)) sent = true
      }
      return sent
    },
    publishRaw(topic, payload, retain = false) {
      let sent = false
      for (const client of clients) {
        if (client?.publishRaw(topic, payload, retain)) sent = true
      }
      return sent
    },
    deleteRetained(topic) {
      let sent = false
      for (const client of clients) {
        if (client?.deleteRetained(topic)) sent = true
      }
      return sent
    },
    subscribe(topic) {
      if (!topic) return false
      extraTopics.add(topic)
      let sent = false
      for (const client of clients) {
        if (client?.subscribe(topic)) sent = true
      }
      return sent
    },
    isReady() {
      return readyCount > 0
    },
    isOpen() {
      return clients.some((c) => Boolean(c?.isOpen()))
    },
    reconnectAll() {
      if (closed) return
      announcedReady = false
      readyCount = 0
      activeCount = 0
      for (let i = 0; i < brokers.length; i += 1) {
        backoffMs[i] = options.reconnectBackoffMs
        spawnBroker(i)
      }
    },
    close() {
      closed = true
      for (let i = 0; i < reconnectTimers.length; i += 1) {
        clearTimer(i)
      }
      for (let i = 0; i < clients.length; i += 1) {
        const c = clients[i]
        clients[i] = null
        c?.close()
      }
    },
  }
}

function parseWire<TState, TIntent, TChat>(payload: string): WireMessage<TState, TIntent, TChat> | null {
  try {
    const msg = JSON.parse(payload) as WireMessage<TState, TIntent, TChat>
    if (!msg || typeof msg !== 'object' || typeof (msg as { type?: unknown }).type !== 'string') {
      return null
    }
    return msg
  } catch {
    return null
  }
}

/**
 * Host a star-topology room over MQTT. Guests on other phones/networks can join
 * without WebRTC NAT traversal.
 */
export function createMqttHostRoom<TState = unknown, TIntent = unknown, TSecret = void, TChat = unknown>(
  callbacks: RoomCallbacks<TState, TIntent, TChat>,
  options: MqttHostOptions<TState, TIntent, TSecret, TChat> = {},
): RoomTransport<TState, TIntent, TSecret, TChat> {
  const cfg = resolveConfig(options)
  const code = options.initialCode
    ? normalizeRoomCode(options.initialCode, cfg.codeLength)
    : generateRoomCode(cfg.codeLength)
  const clientId = options.peerId ?? `${cfg.peerPrefix}${code}`
  const topicTo = (peerId: string) => `${cfg.topicPrefix}/${code}/to/${peerId}`
  const lobbyTopic = `${cfg.topicPrefix}/${code}/lobby`
  const helloIntervalMs = cfg.timing.helloIntervalMs ?? HOST_HELLO_MS
  const peerStaleMs = cfg.timing.peerStaleMs ?? PEER_STALE_MS
  const peerSweepMs = cfg.timing.peerSweepIntervalMs ?? PEER_SWEEP_INTERVAL_MS
  const upsertChat = options.upsertChat ?? defaultChatUpsert
  const isChatSyncIntent = options.isChatSyncIntent ?? defaultIsChatSyncIntent

  let signal: RoomSignalClient | null = null
  let helloTimer: ReturnType<typeof globalThis.setInterval> | null = null
  let sweepTimer: ReturnType<typeof globalThis.setInterval> | null = null
  let reconnectTimer: ReturnType<typeof globalThis.setTimeout> | null = null
  let baselineTimer: ReturnType<typeof globalThis.setTimeout> | null = null
  let lastBaselineAt = 0
  let lastActivityAt = cfg.now()
  let closed = false
  let latestState: TState | null = options.initialState ?? null
  let latestSecret: TSecret | undefined = options.initialSecret
  let broadcastSeq = 0
  let chatHistory: TChat[] = options.initialChatHistory ? [...options.initialChatHistory] : []

  const peers = new Map<string, PeerSlot>()
  const seenIntents = new BoundedIdSet(500)
  const seenControlIds = new BoundedIdSet(500)
  const kickedPeers = new Set<string>()
  const pendingHandoffs = new Map<
    string,
    {
      retryTimer: ReturnType<typeof globalThis.setInterval> | null
      timeoutTimer: ReturnType<typeof globalThis.setTimeout> | null
      resolve: (ok: boolean) => void
    }
  >()

  if (options.initialPeerIds) {
    const t = cfg.now()
    for (const pid of options.initialPeerIds) {
      if (!pid || pid === clientId) continue
      peers.set(pid, {
        peerId: pid,
        lastSeen: t,
        role: 'player',
        ...(options.initialPeerNames?.[pid] ? { name: options.initialPeerNames[pid] } : {}),
      })
    }
  }
  if (options.initialDisplayPeerIds) {
    const t = cfg.now()
    for (const dPid of options.initialDisplayPeerIds) {
      if (!dPid || dPid === clientId) continue
      peers.set(dPid, {
        peerId: dPid,
        lastSeen: t,
        role: 'display',
      })
    }
  }

  const resolveRole = (peerId: string, role?: 'player' | 'display'): 'player' | 'display' =>
    isDisplayPeerId(peerId, cfg.displayPeerPrefix) || role === 'display' ? 'display' : 'player'

  const playerPeerIds = (): string[] =>
    Array.from(peers.values())
      .filter((slot) => slot.role === 'player')
      .map((slot) => slot.peerId)

  const emitStatus = (status: RoomInfo['status'], error = '') => {
    callbacks.onStatus({
      role: 'host',
      code,
      status,
      clientId,
      peers: 1 + playerPeerIds().length,
      error,
    })
  }

  const pushStateTo = (peerId: string) => {
    if (latestState === null || !signal?.isReady()) return
    const isDisplay =
      isDisplayPeerId(peerId, cfg.displayPeerPrefix) || peers.get(peerId)?.role === 'display'
    const filtered = options.sanitizeForPeer
      ? options.sanitizeForPeer(latestState, latestSecret, peerId, isDisplay)
      : latestState
    signal.publish(topicTo(peerId), { type: 'STATE', state: filtered } satisfies WireMessage<TState, TIntent, TChat>)
  }

  const pushChatTo = (peerId: string, message: TChat) => {
    if (!signal?.isReady()) return
    if (
      options.isolateDisplayChat &&
      (isDisplayPeerId(peerId, cfg.displayPeerPrefix) || peers.get(peerId)?.role === 'display')
    ) {
      return
    }
    signal.publish(topicTo(peerId), { type: 'CHAT', message } satisfies WireMessage<TState, TIntent, TChat>)
  }

  const pushChatSyncTo = (peerId: string) => {
    if (chatHistory.length === 0 || !signal?.isReady()) return
    if (
      options.isolateDisplayChat &&
      (isDisplayPeerId(peerId, cfg.displayPeerPrefix) || peers.get(peerId)?.role === 'display')
    ) {
      return
    }
    signal.publish(
      topicTo(peerId),
      { type: 'CHAT_SYNC', messages: chatHistory } satisfies WireMessage<TState, TIntent, TChat>,
    )
  }

  /**
   * Republish the public-safe baseline on the lobby topic (retained, so late joiners and
   * freshly-subscribed guests replay it immediately).
   */
  const publishPublicBaseline = () => {
    if (!signal?.isReady() || latestState === null) return
    lastBaselineAt = cfg.now()
    const baselineState = options.sanitizeForBaseline
      ? options.sanitizeForBaseline(latestState, latestSecret)
      : latestState
    const snapshot: PublicStateSnapshot<TState> = {
      type: 'PUBLIC_STATE',
      state: baselineState,
      updatedAt: cfg.now(),
      hostEpoch: 1,
    }
    signal.publish(lobbyTopic, snapshot, true)
  }

  /** Coalesce baseline publishes — at most one per BASELINE_THROTTLE_MS. */
  const scheduleBaselinePublish = () => {
    if (baselineTimer !== null) return
    const elapsed = cfg.now() - lastBaselineAt
    if (elapsed >= BASELINE_THROTTLE_MS) {
      publishPublicBaseline()
      return
    }
    baselineTimer = globalThis.setTimeout(() => {
      baselineTimer = null
      publishPublicBaseline()
    }, BASELINE_THROTTLE_MS - elapsed)
  }

  const peerNames = (): Record<string, string> => {
    const out: Record<string, string> = {}
    for (const slot of peers.values()) {
      if (slot.role === 'player' && slot.name) {
        out[slot.peerId] = slot.name
      }
    }
    return out
  }

  const notifyPeers = (leftId?: string) => {
    emitStatus(signal?.isReady() ? 'connected' : 'connecting')
    callbacks.onPeerChange?.(playerPeerIds(), leftId, peerNames())
  }

  const removePeer = (peerId: string) => {
    const existing = peers.get(peerId)
    if (!existing) return
    peers.delete(peerId)
    notifyPeers(existing.role === 'player' ? peerId : undefined)
  }

  const touchPeer = (
    peerId: string,
    resendState = false,
    role?: 'player' | 'display',
    rawName?: string,
  ) => {
    const nextRole = resolveRole(peerId, role)
    const cleanName = typeof rawName === 'string' ? rawName.trim().slice(0, cfg.maxPlayerNameLength) : ''
    const existing = peers.get(peerId)
    if (existing) {
      existing.lastSeen = cfg.now()
      if (nextRole === 'display') existing.role = 'display'
      if (cleanName && existing.role === 'player' && cleanName !== existing.name) {
        existing.name = cleanName
        if (options.createJoinIntent) {
          callbacks.onIntent?.(options.createJoinIntent(cleanName), peerId)
        }
      }
      if (resendState) {
        pushStateTo(peerId)
        pushChatSyncTo(peerId)
      }
      return
    }
    peers.set(peerId, {
      peerId,
      lastSeen: cfg.now(),
      role: nextRole,
      ...(cleanName && nextRole === 'player' ? { name: cleanName } : {}),
    })
    const before = broadcastSeq
    notifyPeers()
    if (broadcastSeq === before) {
      pushStateTo(peerId)
    }
    pushChatSyncTo(peerId)
  }

  const publishHello = () => {
    signal?.publish(
      lobbyTopic,
      (options.hostSessionId
        ? { type: 'HELLO', src: clientId, hostSessionId: options.hostSessionId }
        : { type: 'HELLO', src: clientId }) satisfies WireMessage<TState, TIntent, TChat>,
    )
  }

  const clearTimers = () => {
    if (helloTimer !== null) {
      globalThis.clearInterval(helloTimer)
      helloTimer = null
    }
    if (sweepTimer !== null) {
      globalThis.clearInterval(sweepTimer)
      sweepTimer = null
    }
    if (reconnectTimer !== null) {
      globalThis.clearTimeout(reconnectTimer)
      reconnectTimer = null
    }
    if (baselineTimer !== null) {
      globalThis.clearTimeout(baselineTimer)
      baselineTimer = null
    }
  }

  const settleAllHandoffs = (ok: boolean) => {
    for (const [msgId, entry] of pendingHandoffs.entries()) {
      if (entry.retryTimer !== null) globalThis.clearInterval(entry.retryTimer)
      if (entry.timeoutTimer !== null) globalThis.clearTimeout(entry.timeoutTimer)
      pendingHandoffs.delete(msgId)
      entry.resolve(ok)
    }
  }

  const handleMessage = (_topic: string, payload: string) => {
    lastActivityAt = cfg.now()
    const msg = parseWire<TState, TIntent, TChat>(payload)
    if (
      !msg ||
      msg.type === 'HELLO' ||
      msg.type === 'PUBLIC_STATE' ||
      msg.type === 'STATE' ||
      msg.type === 'CHAT' ||
      msg.type === 'CHAT_SYNC' ||
      msg.type === 'KICK' ||
      msg.type === 'ERROR' ||
      msg.type === 'SESSION' ||
      msg.type === 'HOST_HANDOFF' ||
      msg.type === 'TV_ASSIGN' ||
      msg.type === 'TV_ACK'
    ) {
      return
    }

    if (msg.type === 'HOST_HANDOFF_ACK') {
      const pending = pendingHandoffs.get(msg.msgId)
      if (pending) {
        if (pending.retryTimer !== null) globalThis.clearInterval(pending.retryTimer)
        if (pending.timeoutTimer !== null) globalThis.clearTimeout(pending.timeoutTimer)
        pendingHandoffs.delete(msg.msgId)
        pending.resolve(true)
      }
      return
    }

    if ('src' in msg && msg.src && kickedPeers.has(msg.src)) {
      if (msg.type === 'JOIN' || msg.type === 'PING') {
        signal?.publish(topicTo(msg.src), { type: 'KICK', src: clientId } satisfies WireMessage<TState, TIntent, TChat>)
      }
      return
    }

    if (msg.type === 'JOIN' && msg.src && msg.src !== clientId) {
      if (msg.msgId && !seenControlIds.add(`join:${msg.msgId}`)) {
        return
      }
      const resolvedRole = resolveRole(msg.src, msg.role)
      options.onJoin?.(msg.src, {
        role: resolvedRole,
        ...(typeof msg.name === 'string' ? { name: msg.name } : {}),
        ...(typeof msg.playerToken === 'string' ? { playerToken: msg.playerToken } : {}),
      })
      touchPeer(msg.src, true, msg.role, msg.name)
      return
    }
    if (msg.type === 'LEAVE' && msg.src) {
      removePeer(msg.src)
      return
    }
    if (msg.type === 'PING' && msg.src && msg.src !== clientId) {
      touchPeer(msg.src, false, undefined, msg.name)
      return
    }
    if (msg.type === 'TYPING' && msg.src && typeof msg.typing === 'boolean') {
      if (isDisplayPeerId(msg.src, cfg.displayPeerPrefix) || peers.get(msg.src)?.role === 'display') return
      callbacks.onTyping?.(msg.src, msg.typing)
      return
    }
    if (msg.type === 'INTENT' && msg.src && msg.intent !== undefined) {
      touchPeer(msg.src)
      if (typeof msg.id === 'string' && msg.id.length > 0) {
        if (!seenIntents.add(msg.id)) return
      }
      if (isChatSyncIntent(msg.intent)) {
        pushChatSyncTo(msg.src)
        return
      }
      if (isDisplayPeerId(msg.src, cfg.displayPeerPrefix) || peers.get(msg.src)?.role === 'display') {
        return
      }
      callbacks.onIntent?.(msg.intent, msg.src)
    }
  }

  const subscribeTopics = [topicTo(clientId), lobbyTopic]
  if (options.fallbackHostId && options.fallbackHostId !== clientId) {
    subscribeTopics.push(topicTo(options.fallbackHostId))
  }

  const connectSignal = () => {
    if (closed) return
    clearTimers()
    signal?.close()
    emitStatus('connecting')

    signal = connectRoomSignal({
      code,
      codeLength: cfg.codeLength,
      brokers: cfg.brokers,
      clientId,
      subscribeTopics,
      WebSocketImpl: cfg.WebSocketImpl,
      reconnectBackoffMs: cfg.reconnectBackoffMs,
      maxReconnectBackoffMs: cfg.maxReconnectBackoffMs,
      onReady() {
        lastActivityAt = cfg.now()
        emitStatus('connected')
        publishHello()
        if (helloTimer !== null) {
          globalThis.clearInterval(helloTimer)
          helloTimer = null
        }
        if (sweepTimer !== null) {
          globalThis.clearInterval(sweepTimer)
          sweepTimer = null
        }
        helloTimer = globalThis.setInterval(publishHello, helloIntervalMs)
        sweepTimer = globalThis.setInterval(() => {
          const t = cfg.now()
          for (const [peerId, slot] of peers) {
            if (t - slot.lastSeen > peerStaleMs) removePeer(peerId)
          }
        }, peerSweepMs)
        // Resume (and initial) broadcast: re-push fresh STATE + chat history to every
        // known guest and refresh the retained public-safe baseline.
        for (const peerId of peers.keys()) {
          pushStateTo(peerId)
          pushChatSyncTo(peerId)
        }
        publishPublicBaseline()
      },
      onMessage: handleMessage,
      onDisconnect() {
        if (closed) return
        emitStatus('connecting')
        reconnectTimer = globalThis.setTimeout(() => {
          connectSignal()
        }, 2_000)
      },
    })
  }

  const detachWake = attachWakeListeners(cfg.doc, cfg.win, () => {
    if (closed) return
    const elapsed = cfg.now() - lastActivityAt
    if (elapsed > cfg.wakeReconnectMs || !signal?.isReady()) {
      signal?.reconnectAll()
    } else {
      publishHello()
    }
    lastActivityAt = cfg.now()
  })

  connectSignal()

  return {
    broadcast(state: TState, secret?: TSecret) {
      broadcastSeq += 1
      latestState = state
      latestSecret = secret
      for (const peerId of peers.keys()) {
        pushStateTo(peerId)
      }
      scheduleBaselinePublish()
    },
    broadcastChat(message: TChat) {
      chatHistory = upsertChat(chatHistory, message)
      for (const peerId of peers.keys()) {
        pushChatTo(peerId, message)
      }
    },
    sendIntent() {
      // Host applies intents locally.
    },
    sendTyping(typing: boolean) {
      if (!signal?.isReady()) return
      signal.publish(lobbyTopic, { type: 'TYPING', src: clientId, typing } satisfies WireMessage<TState, TIntent, TChat>)
    },
    kickPeer(peerId: string) {
      if (!peerId || peerId === clientId) return
      kickedPeers.add(peerId)
      if (signal?.isReady()) {
        signal.publish(
          topicTo(peerId),
          { type: 'KICK', msgId: createMessageId(), src: clientId } satisfies WireMessage<TState, TIntent, TChat>,
        )
      }
      removePeer(peerId)
    },
    sendError(peerId: string, error: RoomError) {
      if (closed) return
      if (peerId === clientId) {
        callbacks.onError?.(error)
        return
      }
      if (!signal?.isReady()) return
      signal.publish(
        topicTo(peerId),
        { type: 'ERROR', msgId: createMessageId(), src: clientId, error } satisfies WireMessage<TState, TIntent, TChat>,
      )
    },
    sendSession(peerId: string, playerId: string, playerToken: string) {
      if (closed) return
      if (peerId === clientId) {
        callbacks.onSessionToken?.(playerId, playerToken)
        return
      }
      if (!signal?.isReady()) return
      signal.publish(
        topicTo(peerId),
        {
          type: 'SESSION',
          msgId: createMessageId(),
          src: clientId,
          playerId,
          playerToken,
        } satisfies WireMessage<TState, TIntent, TChat>,
      )
    },
    initiateHostHandoff(
      targetPeerId: string,
      payload: HostHandoffPayload<TState, TChat>,
      handoffOpts?: { retryMs?: number; timeoutMs?: number },
    ): Promise<boolean> {
      if (closed || !targetPeerId || targetPeerId === clientId) {
        return Promise.resolve(false)
      }
      const msgId = createMessageId()
      const retryMs = handoffOpts?.retryMs ?? 400
      const timeoutMs = handoffOpts?.timeoutMs ?? 5_000

      return new Promise((resolve) => {
        const sendPacket = () => {
          if (closed) return
          signal?.publish(
            topicTo(targetPeerId),
            {
              type: 'HOST_HANDOFF',
              msgId,
              src: clientId,
              payload,
            } satisfies WireMessage<TState, TIntent, TChat>,
          )
        }

        const retryTimer = globalThis.setInterval(sendPacket, retryMs)
        const timeoutTimer = globalThis.setTimeout(() => {
          const entry = pendingHandoffs.get(msgId)
          if (!entry) return
          if (entry.retryTimer !== null) globalThis.clearInterval(entry.retryTimer)
          pendingHandoffs.delete(msgId)
          resolve(false)
        }, timeoutMs)

        pendingHandoffs.set(msgId, { retryTimer, timeoutTimer, resolve })
        sendPacket()
      })
    },
    requestSync() {
      if (closed) return
      if (!signal?.isReady()) {
        signal?.reconnectAll()
        return
      }
      publishHello()
      for (const peerId of peers.keys()) {
        pushStateTo(peerId)
        pushChatSyncTo(peerId)
      }
    },
    close() {
      closed = true
      settleAllHandoffs(false)
      clearTimers()
      detachWake()
      if (signal?.isReady()) {
        for (const peerId of peers.keys()) {
          signal.publish(topicTo(peerId), { type: 'LEAVE', src: clientId } satisfies WireMessage<TState, TIntent, TChat>)
        }
        signal.publish(lobbyTopic, { type: 'LEAVE', src: clientId } satisfies WireMessage<TState, TIntent, TChat>)
        signal.deleteRetained(lobbyTopic)
      }
      peers.clear()
      kickedPeers.clear()
      chatHistory = []
      signal?.close()
      signal = null
    },
  }
}

function createMqttSubscriberRoom<TState, TIntent, TSecret = void, TChat = unknown>(
  rawCode: string,
  callbacks: RoomCallbacks<TState, TIntent, TChat>,
  role: 'guest' | 'display',
  options: MqttSubscriberOptions<TState, TIntent, TChat> = {},
): RoomTransport<TState, TIntent, TSecret, TChat> {
  const cfg = resolveConfig(options)
  const code = normalizeRoomCode(rawCode, cfg.codeLength)
  let activeHostId = `${cfg.peerPrefix}${code}`
  let activeHostSessionId: string | undefined = undefined
  const isDisplay = role === 'display'
  const clientId =
    options.peerId ??
    (isDisplay ? `${cfg.displayPeerPrefix}${randomToken(7)}` : `${cfg.peerPrefix}g-${randomToken(7)}`)
  const topicTo = (peerId: string) => `${cfg.topicPrefix}/${code}/to/${peerId}`
  const lobbyTopic = `${cfg.topicPrefix}/${code}/lobby`
  const hostDownAfterMs = cfg.timing.hostDownAfterMs ?? HOST_DOWN_AFTER_MS
  const hostCheckIntervalMs = cfg.timing.hostCheckIntervalMs ?? HOST_CHECK_INTERVAL_MS
  const guestReconnectDelayMs = cfg.timing.guestReconnectDelayMs ?? GUEST_RECONNECT_DELAY_MS
  const connectTimeoutMs = cfg.timing.connectTimeoutMs ?? CONNECT_TIMEOUT_MS
  const peerPingMs = cfg.timing.peerPingMs ?? PEER_PING_MS
  const joinRetryMs = cfg.timing.joinRetryMs ?? JOIN_RETRY_MS
  const isDisplayAllowedIntent = options.isDisplayAllowedIntent ?? defaultIsChatSyncIntent

  let signal: RoomSignalClient | null = null
  let timeoutTimer: ReturnType<typeof globalThis.setTimeout> | null = null
  let joinTimer: ReturnType<typeof globalThis.setInterval> | null = null
  let pingTimer: ReturnType<typeof globalThis.setInterval> | null = null
  let hostCheckTimer: ReturnType<typeof globalThis.setInterval> | null = null
  let reconnectTimer: ReturnType<typeof globalThis.setTimeout> | null = null
  let closed = false
  let everConnected = false
  let connectedAnnounced = false
  let freshStateReceived = false
  let lastHelloAt = 0
  let lastActivityAt = cfg.now()
  let lastDeliveredVersion = Number.NEGATIVE_INFINITY
  let hostDown = false
  let intentSeq = 0
  const seenControlIds = new BoundedIdSet(500)

  const emitStatus = (status: RoomInfo['status'], error = '') => {
    callbacks.onStatus({
      role,
      code,
      status,
      clientId,
      peers: status === 'connected' ? 2 : 1,
      error,
      ...(hostDown ? { hostDown: true } : {}),
    })
  }

  const clearTimers = () => {
    if (timeoutTimer !== null) {
      globalThis.clearTimeout(timeoutTimer)
      timeoutTimer = null
    }
    if (joinTimer !== null) {
      globalThis.clearInterval(joinTimer)
      joinTimer = null
    }
    if (pingTimer !== null) {
      globalThis.clearInterval(pingTimer)
      pingTimer = null
    }
    if (hostCheckTimer !== null) {
      globalThis.clearInterval(hostCheckTimer)
      hostCheckTimer = null
    }
    if (reconnectTimer !== null) {
      globalThis.clearTimeout(reconnectTimer)
      reconnectTimer = null
    }
  }

  const currentName = (): string | undefined => {
    if (isDisplay) return undefined
    const clean = callbacks.getPlayerName?.().trim().slice(0, cfg.maxPlayerNameLength)
    return clean || undefined
  }

  const currentToken = (): string | undefined => {
    if (isDisplay) return undefined
    const tok = options.getPlayerToken?.()
    return tok && tok.trim().length > 0 ? tok : undefined
  }

  const startPingLoop = () => {
    if (pingTimer !== null) {
      globalThis.clearInterval(pingTimer)
      pingTimer = null
    }
    pingTimer = globalThis.setInterval(() => {
      const name = currentName()
      const playerToken = currentToken()
      signal?.publish(
        topicTo(activeHostId),
        {
          type: 'PING',
          src: clientId,
          ...(name ? { name } : {}),
          ...(playerToken ? { playerToken } : {}),
        } satisfies WireMessage<TState, TIntent, TChat>,
      )
    }, peerPingMs)
  }

  const publishJoin = () => {
    const name = currentName()
    const playerToken = currentToken()
    signal?.publish(
      topicTo(activeHostId),
      (isDisplay
        ? { type: 'JOIN', msgId: createMessageId(), src: clientId, role: 'display' }
        : {
            type: 'JOIN',
            msgId: createMessageId(),
            src: clientId,
            ...(name ? { name } : {}),
            ...(playerToken ? { playerToken } : {}),
          }) satisfies WireMessage<TState, TIntent, TChat>,
    )
  }

  const startJoinLoop = () => {
    if (joinTimer !== null) {
      globalThis.clearInterval(joinTimer)
      joinTimer = null
    }
    joinTimer = globalThis.setInterval(publishJoin, joinRetryMs)
  }

  const checkLiveness = () => {
    if (closed || !everConnected || lastHelloAt === 0) return
    if (!hostDown && cfg.now() - lastHelloAt >= hostDownAfterMs) {
      hostDown = true
      emitStatus('connected')
    }
  }

  const fail = (message: string) => {
    if (closed) return
    clearTimers()
    emitStatus('error', message)
  }

  const markConnected = () => {
    if (closed) return
    const wasHostDown = hostDown
    everConnected = true
    if (timeoutTimer !== null) {
      globalThis.clearTimeout(timeoutTimer)
      timeoutTimer = null
    }
    if (joinTimer !== null) {
      globalThis.clearInterval(joinTimer)
      joinTimer = null
    }
    hostDown = false
    if (!connectedAnnounced || wasHostDown) {
      connectedAnnounced = true
      emitStatus('connected')
    }
  }

  const startHostCheck = () => {
    if (hostCheckTimer !== null) return
    hostCheckTimer = globalThis.setInterval(checkLiveness, hostCheckIntervalMs)
  }

  const deliverSnapshot = (state: TState) => {
    if (options.getStateVersion) {
      const ver = options.getStateVersion(state)
      if (ver <= lastDeliveredVersion) return
      lastDeliveredVersion = ver
    }
    callbacks.onSnapshot?.(state)
  }

  const connectSignal = () => {
    if (closed) return
    signal?.close()
    signal = connectRoomSignal({
      code,
      codeLength: cfg.codeLength,
      brokers: cfg.brokers,
      clientId,
      subscribeTopics: [topicTo(clientId), lobbyTopic],
      WebSocketImpl: cfg.WebSocketImpl,
      reconnectBackoffMs: cfg.reconnectBackoffMs,
      maxReconnectBackoffMs: cfg.maxReconnectBackoffMs,
      onReady() {
        lastActivityAt = cfg.now()
        publishJoin()
        startJoinLoop()
        startPingLoop()
        startHostCheck()
      },
      onMessage(_topic, payload) {
        lastActivityAt = cfg.now()
        const msg = parseWire<TState, TIntent, TChat>(payload)
        if (!msg) return

        if (msg.type === 'HELLO' && msg.src) {
          const isExpectedHost = msg.src === activeHostId
          const hostChanged = Boolean(msg.src !== activeHostId && msg.hostSessionId !== undefined)
          const sessionChanged = Boolean(msg.hostSessionId && msg.hostSessionId !== activeHostSessionId)

          if (!isExpectedHost && !hostChanged) {
            return
          }

          if (hostChanged || sessionChanged) {
            activeHostId = msg.src
            if (msg.hostSessionId) activeHostSessionId = msg.hostSessionId
            callbacks.onHostChanged?.(activeHostId, activeHostSessionId)
          }

          const wasDown = hostDown
          lastHelloAt = cfg.now()
          hostDown = false
          if (wasDown) {
            // Host is back — clear the standby banner and pull a fresh snapshot via JOIN.
            emitStatus('connected')
            publishJoin()
            return
          }
          if (!everConnected || hostChanged || sessionChanged) {
            publishJoin()
          }
          return
        }
        if (msg.type === 'PUBLIC_STATE' && msg.state !== undefined) {
          // Replay the retained baseline as a fallback — fresh live STATE below always wins.
          if (freshStateReceived) return
          const snapshot = msg as PublicStateSnapshot<TState>
          if (!isExpiredPublicSnapshot(snapshot, cfg.now())) {
            const safeState = options.sanitizeBaselineOnReceive
              ? options.sanitizeBaselineOnReceive(snapshot.state)
              : snapshot.state
            deliverSnapshot(safeState)
          }
          return
        }
        if (msg.type === 'STATE' && msg.state !== undefined) {
          freshStateReceived = true
          markConnected()
          deliverSnapshot(msg.state)
          return
        }
        if (msg.type === 'SESSION' && !isDisplay) {
          if (msg.msgId && !seenControlIds.add(`session:${msg.msgId}`)) return
          callbacks.onSessionToken?.(msg.playerId, msg.playerToken)
          return
        }
        if (msg.type === 'ERROR') {
          if (msg.msgId && !seenControlIds.add(`err:${msg.msgId}`)) return
          callbacks.onError?.(msg.error)
          return
        }
        if (msg.type === 'HOST_HANDOFF' && !isDisplay) {
          // Always ACK back to outgoing host (even on duplicate broker deliveries).
          signal?.publish(
            topicTo(msg.src),
            { type: 'HOST_HANDOFF_ACK', msgId: msg.msgId, src: clientId } satisfies WireMessage<TState, TIntent, TChat>,
          )
          if (!seenControlIds.add(`handoff:${msg.msgId}`)) return
          callbacks.onHostHandoff?.(msg.payload, msg.src)
          return
        }
        if (msg.type === 'CHAT') {
          if (options.isolateDisplayChat && isDisplay) return
          if (msg.msgId && !seenControlIds.add(`chat:${msg.msgId}`)) return
          if (options.validateChat) {
            if (options.validateChat(msg.message)) callbacks.onChat?.(msg.message)
          } else if (msg.message !== undefined && msg.message !== null) {
            callbacks.onChat?.(msg.message)
          }
          return
        }
        if (msg.type === 'CHAT_SYNC' && Array.isArray(msg.messages)) {
          if (options.isolateDisplayChat && isDisplay) return
          if (msg.msgId && !seenControlIds.add(`chatsync:${msg.msgId}`)) return
          const valid = options.validateChat ? msg.messages.filter(options.validateChat) : msg.messages
          callbacks.onChatSync?.(valid)
          return
        }
        if (msg.type === 'TYPING' && msg.src && typeof msg.typing === 'boolean') {
          if (options.isolateDisplayChat && isDisplay) return
          callbacks.onTyping?.(msg.src, msg.typing)
          return
        }
        if (msg.type === 'KICK' && (msg.src === activeHostId || msg.src === `${cfg.peerPrefix}${code}`)) {
          if (msg.msgId && !seenControlIds.add(`kick:${msg.msgId}`)) return
          callbacks.onError?.({ code: 'KICKED', message: 'Removed from the room by the host.' })
          fail('Removed from the room by the host.')
          return
        }
        if (msg.type === 'LEAVE' && msg.src === activeHostId) {
          callbacks.onError?.({ code: 'HOST_LEFT', message: 'Host closed the room.' })
          fail('Host closed the room.')
        }
      },
      onDisconnect(hadConnected) {
        if (closed) return
        if (!everConnected) {
          fail(hadConnected ? `Could not connect to room ${code}.` : 'Could not reach signaling server.')
          return
        }
        // Own sockets dropping must not tear down a live room — show the standby banner
        // and reconnect in the background.
        if (!hostDown) {
          hostDown = true
          emitStatus('connected')
        }
        startHostCheck()
        if (reconnectTimer !== null) {
          globalThis.clearTimeout(reconnectTimer)
        }
        reconnectTimer = globalThis.setTimeout(() => {
          reconnectTimer = null
          connectSignal()
        }, guestReconnectDelayMs)
      },
    })
  }

  const wakeAndResync = () => {
    if (closed) return
    const elapsed = cfg.now() - lastActivityAt
    if (elapsed > cfg.wakeReconnectMs || !signal?.isReady()) {
      signal?.reconnectAll()
    } else {
      publishJoin()
    }
    lastActivityAt = cfg.now()
  }

  const detachWake = attachWakeListeners(cfg.doc, cfg.win, wakeAndResync)

  emitStatus('connecting')

  timeoutTimer = globalThis.setTimeout(() => {
    if (!everConnected) {
      fail(`Room ${code} did not respond. Make sure the host tab is open.`)
    }
  }, connectTimeoutMs)

  connectSignal()

  return {
    broadcast() {
      // Only the host broadcasts snapshots.
    },
    broadcastChat() {
      // Only the host broadcasts chat messages.
    },
    sendTyping(typing: boolean) {
      if (isDisplay || !signal?.isReady()) return
      signal.publish(lobbyTopic, { type: 'TYPING', src: clientId, typing } satisfies WireMessage<TState, TIntent, TChat>)
    },
    sendIntent(intent: TIntent) {
      if (isDisplay && !isDisplayAllowedIntent(intent)) return
      intentSeq += 1
      const playerToken = currentToken()
      signal?.publish(topicTo(activeHostId), {
        type: 'INTENT',
        src: clientId,
        id: `${clientId}:${intentSeq}`,
        ...(playerToken ? { playerToken } : {}),
        intent,
      } satisfies WireMessage<TState, TIntent, TChat>)
    },
    kickPeer() {
      // Only the host can remove players.
    },
    requestSync() {
      wakeAndResync()
    },
    close() {
      closed = true
      clearTimers()
      detachWake()
      if (signal?.isReady()) {
        const playerToken = currentToken()
        signal.publish(topicTo(activeHostId), {
          type: 'LEAVE',
          src: clientId,
          ...(playerToken ? { playerToken } : {}),
        } satisfies WireMessage<TState, TIntent, TChat>)
      }
      signal?.close()
      signal = null
    },
  }
}

/** Join a room hosted on another device via MQTT. */
export function createMqttGuestRoom<TState = unknown, TIntent = unknown, TSecret = void, TChat = unknown>(
  rawCode: string,
  callbacks: RoomCallbacks<TState, TIntent, TChat>,
  options?: MqttSubscriberOptions<TState, TIntent, TChat>,
): RoomTransport<TState, TIntent, TSecret, TChat> {
  return createMqttSubscriberRoom<TState, TIntent, TSecret, TChat>(rawCode, callbacks, 'guest', options)
}

/** Connect a read-only TV / big-screen display to a room via MQTT. */
export function createMqttDisplayRoom<TState = unknown, TIntent = unknown, TSecret = void, TChat = unknown>(
  rawCode: string,
  callbacks: RoomCallbacks<TState, TIntent, TChat>,
  options?: MqttSubscriberOptions<TState, TIntent, TChat>,
): RoomTransport<TState, TIntent, TSecret, TChat> {
  return createMqttSubscriberRoom<TState, TIntent, TSecret, TChat>(rawCode, callbacks, 'display', options)
}

/** Listen on a TV pairing channel so another device can set which room this TV displays. */
export function createMqttTvListener(
  tvCode: string,
  callbacks: {
    onReady?: () => void
    onAssignRoom: (roomCode: string) => void
    onDisconnect?: () => void
  },
  config?: MqttRoomConfig,
): { close: () => void } {
  const cfg = resolveConfig(config)
  const cleanTv = normalizeRoomCode(tvCode, cfg.codeLength)
  const tvTopic = `${cfg.topicPrefix}/tv/${cleanTv}`
  const clientId = `${cfg.peerPrefix}t-${randomToken(7)}`
  let signal: RoomSignalClient | null = null
  let reconnectTimer: ReturnType<typeof globalThis.setTimeout> | null = null
  let closed = false
  let lastAssignedCode = ''
  let lastAssignedAt = 0

  const clearReconnect = () => {
    if (reconnectTimer !== null) {
      globalThis.clearTimeout(reconnectTimer)
      reconnectTimer = null
    }
  }

  const connect = () => {
    if (closed || cleanTv.length !== cfg.codeLength) return
    clearReconnect()
    signal?.close()
    signal = connectRoomSignal({
      code: cleanTv,
      codeLength: cfg.codeLength,
      brokers: cfg.brokers,
      clientId,
      subscribeTopics: [tvTopic],
      WebSocketImpl: cfg.WebSocketImpl,
      reconnectBackoffMs: cfg.reconnectBackoffMs,
      maxReconnectBackoffMs: cfg.maxReconnectBackoffMs,
      onReady() {
        if (!closed) callbacks.onReady?.()
      },
      onMessage(_topic, payload) {
        if (closed) return
        const msg = parseWire(payload)
        if (!msg || msg.type !== 'TV_ASSIGN') return
        const cleanRoom = normalizeRoomCode(msg.roomCode, cfg.codeLength)
        if (cleanRoom.length !== cfg.codeLength) return
        signal?.publish(
          tvTopic,
          { type: 'TV_ACK', roomCode: cleanRoom, tvCode: cleanTv } satisfies WireMessage,
        )
        const t = cfg.now()
        if (cleanRoom === lastAssignedCode && t - lastAssignedAt < 3_000) return
        lastAssignedCode = cleanRoom
        lastAssignedAt = t
        callbacks.onAssignRoom(cleanRoom)
      },
      onDisconnect() {
        if (closed) return
        callbacks.onDisconnect?.()
        reconnectTimer = globalThis.setTimeout(() => {
          connect()
        }, 2_000)
      },
    })
  }

  connect()

  return {
    close() {
      closed = true
      clearReconnect()
      signal?.close()
      signal = null
    },
  }
}

/** Send a room code to a TV display listening on its pairing channel, returning full outcome status. */
export function assignRoomToTvDisplayDetailed(
  tvCode: string,
  roomCode: string,
  options?: MqttRoomConfig & {
    retryMs?: number
    optimisticMs?: number
    timeoutMs?: number
  },
): Promise<TvAssignOutcome> {
  const cfg = resolveConfig(options)
  const cleanTv = normalizeRoomCode(tvCode, cfg.codeLength)
  const cleanRoom = normalizeRoomCode(roomCode, cfg.codeLength)
  if (cleanTv.length !== cfg.codeLength || cleanRoom.length !== cfg.codeLength) {
    return Promise.resolve({
      status: 'timeout',
      tvCode: cleanTv,
      roomCode: cleanRoom,
    })
  }

  const tvTopic = `${cfg.topicPrefix}/tv/${cleanTv}`
  const retryMs = options?.retryMs ?? 400
  const optimisticMs = options?.optimisticMs ?? 1_200
  const timeoutMs = options?.timeoutMs ?? 8_000
  const clientId = `${cfg.peerPrefix}c-${randomToken(7)}`

  return new Promise((resolve) => {
    let settled = false
    let hasPublished = false
    let retryTimer: ReturnType<typeof globalThis.setInterval> | null = null
    let optimisticTimer: ReturnType<typeof globalThis.setTimeout> | null = null
    let timeoutTimer: ReturnType<typeof globalThis.setTimeout> | null = null
    let signal: RoomSignalClient | null = null

    const cleanup = () => {
      if (retryTimer !== null) {
        globalThis.clearInterval(retryTimer)
        retryTimer = null
      }
      if (optimisticTimer !== null) {
        globalThis.clearTimeout(optimisticTimer)
        optimisticTimer = null
      }
      if (timeoutTimer !== null) {
        globalThis.clearTimeout(timeoutTimer)
        timeoutTimer = null
      }
      signal?.close()
      signal = null
    }

    const finish = (status: TvAssignOutcome['status']) => {
      if (settled) return
      settled = true
      cleanup()
      resolve({ status, tvCode: cleanTv, roomCode: cleanRoom })
    }

    const publishAssign = () => {
      if (!signal?.isReady()) return
      hasPublished = true
      signal.publish(
        tvTopic,
        { type: 'TV_ASSIGN', roomCode: cleanRoom, src: clientId } satisfies WireMessage,
      )
    }

    timeoutTimer = globalThis.setTimeout(() => {
      finish(hasPublished ? 'optimistic' : 'timeout')
    }, timeoutMs)

    signal = connectRoomSignal({
      code: cleanTv,
      codeLength: cfg.codeLength,
      brokers: cfg.brokers,
      clientId,
      subscribeTopics: [tvTopic],
      WebSocketImpl: cfg.WebSocketImpl,
      reconnectBackoffMs: cfg.reconnectBackoffMs,
      maxReconnectBackoffMs: cfg.maxReconnectBackoffMs,
      onReady() {
        if (settled) return
        publishAssign()
        if (settled) return
        retryTimer = globalThis.setInterval(publishAssign, retryMs)
        optimisticTimer = globalThis.setTimeout(() => {
          finish(hasPublished ? 'optimistic' : 'timeout')
        }, optimisticMs)
      },
      onMessage(_topic, payload) {
        if (settled) return
        const msg = parseWire(payload)
        if (!msg || msg.type !== 'TV_ACK') return
        if (
          normalizeRoomCode(msg.roomCode, cfg.codeLength) === cleanRoom &&
          normalizeRoomCode(msg.tvCode, cfg.codeLength) === cleanTv
        ) {
          finish('acknowledged')
        }
      },
      onDisconnect() {
        finish(hasPublished ? 'optimistic' : 'timeout')
      },
    })
  })
}

/** Send a room code to a TV display listening on its pairing channel. */
export function assignRoomToTvDisplay(
  tvCode: string,
  roomCode: string,
  options?: MqttRoomConfig & { timeoutMs?: number },
): Promise<boolean> {
  return assignRoomToTvDisplayDetailed(tvCode, roomCode, options).then(
    (outcome) => outcome.status !== 'timeout',
  )
}
