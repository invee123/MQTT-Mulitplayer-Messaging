import {
  DEFAULT_TOPIC_PREFIX,
  attachWakeListeners,
  brokersForRoom,
  resolveConfig,
} from './brokers.js'
import { ROOM_CODE_LENGTH, normalizeRoomCode, randomToken } from './codes.js'
import { connectMqttSignal, type MqttSignalClient } from './signal.js'
import { PUBLIC_STATE_TTL_MS, type MqttRoomConfig, type StorageLike } from './types.js'

export const PEER_HEARTBEAT_MS = 8_000
export const PEER_STALE_MS = 25_000
export const PEER_SWEEP_MS = 5_000

export type PeerRoomConnectionStatus = 'connecting' | 'connected' | 'reconnecting' | 'error'

export type PeerStateMergeResult<TState> = {
  state: TState
  changed: boolean
  incomingWasBehind: boolean
}

export type MqttPeerRoomOptions<TState> = MqttRoomConfig & {
  roomCode: string
  clientId?: string
  initialState?: TState
  publishInitialState?: boolean
  createEmptyState: (roomCode: string) => TState
  mergeState: (local: TState, incoming: TState) => PeerStateMergeResult<TState>
  validateState?: (value: unknown, roomCode: string) => value is TState
  isExpiredState?: (state: TState, now: number, ttlMs: number) => boolean
  hasState?: (state: TState) => boolean
  storageKey?: string
  storage?: StorageLike | null
  rng?: () => number
  heartbeatMs?: number
  peerStaleMs?: number
  peerSweepMs?: number
  roomTtlMs?: number
  onState: (state: TState) => void
  onStatus: (status: PeerRoomConnectionStatus) => void
  onPeerCount: (count: number) => void
  onError?: (message: string) => void
}

export type MqttPeerRoom<TState> = {
  readonly roomCode: string
  readonly clientId: string
  getState: () => TState
  publishState: (nextState: TState) => void
  requestSync: () => void
  close: (opts?: { clearIfLast?: boolean }) => void
}

type SyncWireMessage =
  | { type: 'SYNC_REQ'; src: string; roomCode: string; ts: number }
  | { type: 'HERE'; src: string; roomCode: string; ts: number }
  | { type: 'BYE'; src: string; roomCode: string }

type StateWireMessage<TState> = {
  type: 'STATE'
  src: string
  state: TState
}

export function roomStateTopic(
  roomCode: string,
  topicPrefix = DEFAULT_TOPIC_PREFIX,
  codeLength = ROOM_CODE_LENGTH,
): string {
  const cleanPrefix = topicPrefix.replace(/\/+$/, '')
  return `${cleanPrefix}/${normalizeRoomCode(roomCode, codeLength)}/state`
}

export function roomSyncTopic(
  roomCode: string,
  topicPrefix = DEFAULT_TOPIC_PREFIX,
  codeLength = ROOM_CODE_LENGTH,
): string {
  const cleanPrefix = topicPrefix.replace(/\/+$/, '')
  return `${cleanPrefix}/${normalizeRoomCode(roomCode, codeLength)}/sync`
}

function resolveStorage(storage?: StorageLike | null): StorageLike | null {
  if (storage !== undefined) return storage
  try {
    if (typeof globalThis.localStorage !== 'undefined' && globalThis.localStorage !== null) {
      return globalThis.localStorage
    }
  } catch {
    // Ignore storage access errors in restricted contexts.
  }
  return null
}

function loadFromStorage<TState>(
  storage: StorageLike | null,
  key: string | undefined,
  roomCode: string,
  nowMs: number,
  ttlMs: number,
  validateState?: (value: unknown, code: string) => value is TState,
  isExpiredState?: (state: TState, now: number, ttlMs: number) => boolean,
): TState | null {
  if (!storage || !key) return null
  try {
    const raw = storage.getItem(key)
    if (!raw) return null
    const parsed: unknown = JSON.parse(raw)
    if (validateState && !validateState(parsed, roomCode)) return null
    if (!parsed || typeof parsed !== 'object') return null
    const candidate = parsed as TState
    if (isExpiredState?.(candidate, nowMs, ttlMs)) {
      storage.removeItem?.(key)
      return null
    }
    return candidate
  } catch {
    return null
  }
}

function saveToStorage<TState>(storage: StorageLike | null, key: string | undefined, state: TState): void {
  if (!storage || !key) return
  try {
    storage.setItem(key, JSON.stringify(state))
  } catch {
    // Ignore storage quota / privacy mode errors.
  }
}

function clearFromStorage(storage: StorageLike | null, key: string | undefined): void {
  if (!storage || !key) return
  try {
    storage.removeItem?.(key)
  } catch {
    // Ignore storage removal errors.
  }
}

function parseJsonObject(payload: string): Record<string, unknown> | null {
  try {
    const parsed: unknown = JSON.parse(payload)
    if (typeof parsed !== 'object' || parsed === null) return null
    return parsed as Record<string, unknown>
  } catch {
    return null
  }
}

/**
 * Hostless peer-replicated room transport over MQTT-over-WSS.
 * Every peer holds the shared state, publishes retained snapshots, merges concurrent
 * updates via a deterministic CRDT `mergeState`, and recovers cleanly from mobile sleep.
 */
export function createMqttPeerRoom<TState>(options: MqttPeerRoomOptions<TState>): MqttPeerRoom<TState> {
  const cfg = resolveConfig(options)
  const roomCode = normalizeRoomCode(options.roomCode, cfg.codeLength)
  const clientId = options.clientId ?? `${cfg.peerPrefix}${randomToken(10)}`
  const now = cfg.now
  const rng = options.rng ?? Math.random
  const heartbeatMs = options.heartbeatMs ?? PEER_HEARTBEAT_MS
  const peerStaleMs = options.peerStaleMs ?? PEER_STALE_MS
  const peerSweepMs = options.peerSweepMs ?? PEER_SWEEP_MS
  const roomTtlMs = options.roomTtlMs ?? PUBLIC_STATE_TTL_MS
  const hasState = options.hasState ?? ((s: TState) => Boolean(s))
  const storage = resolveStorage(options.storage)

  const stateTopic = roomStateTopic(roomCode, cfg.topicPrefix, cfg.codeLength)
  const syncTopic = roomSyncTopic(roomCode, cfg.topicPrefix, cfg.codeLength)
  const brokerUrls = brokersForRoom(roomCode, cfg.brokers, cfg.codeLength)

  let currentState = options.createEmptyState(roomCode)
  const persisted = loadFromStorage(
    storage,
    options.storageKey,
    roomCode,
    now(),
    roomTtlMs,
    options.validateState,
    options.isExpiredState,
  )
  if (persisted) {
    currentState = persisted
  }
  if (options.initialState) {
    const valid = options.validateState ? options.validateState(options.initialState, roomCode) : true
    const expired = options.isExpiredState
      ? options.isExpiredState(options.initialState, now(), roomTtlMs)
      : false
    if (valid && !expired) {
      currentState = options.mergeState(currentState, options.initialState).state
    }
  }
  if (hasState(currentState)) {
    saveToStorage(storage, options.storageKey, currentState)
  }

  let closed = false
  let everConnected = false
  let lastStatus: PeerRoomConnectionStatus | null = null
  let lastPeerCount = 1
  let lastActivityAt = now()
  let shouldPublishInitialOnReady = Boolean(options.publishInitialState)
  let connAttempt = 0

  const clients: Array<MqttSignalClient | null> = brokerUrls.map(() => null)
  const reconnectTimers: Array<ReturnType<typeof globalThis.setTimeout> | null> = brokerUrls.map(() => null)
  const brokerFailedOnce: boolean[] = brokerUrls.map(() => false)
  const brokerBackoffMs: number[] = brokerUrls.map(() => cfg.reconnectBackoffMs)
  const peerLastSeen = new Map<string, number>()

  let heartbeatTimer: ReturnType<typeof globalThis.setInterval> | null = null
  let sweepTimer: ReturnType<typeof globalThis.setInterval> | null = null
  let syncReplyTimer: ReturnType<typeof globalThis.setTimeout> | null = null
  let catchupPublishTimer: ReturnType<typeof globalThis.setTimeout> | null = null

  const emitStatus = (next: PeerRoomConnectionStatus) => {
    if (lastStatus === next) return
    lastStatus = next
    options.onStatus(next)
  }

  const emitPeerCount = () => {
    const count = 1 + peerLastSeen.size
    if (count === lastPeerCount) return
    lastPeerCount = count
    options.onPeerCount(count)
  }

  const markPeerSeen = (peerId: string) => {
    if (!peerId || peerId === clientId) return
    peerLastSeen.set(peerId, now())
    emitPeerCount()
  }

  const hasReadyClient = () => clients.some((c) => Boolean(c && c.isReady()))

  const publishRawToAll = (topic: string, rawPayload: string, retain = false): boolean => {
    if (closed) return false
    let sent = false
    for (const client of clients) {
      if (client && client.isReady() && client.publishRaw(topic, rawPayload, retain)) {
        sent = true
      }
    }
    if (!sent) {
      for (const client of clients) {
        if (client && client.isOpen() && client.publishRaw(topic, rawPayload, retain)) {
          sent = true
        }
      }
    }
    return sent
  }

  const publishToAll = (topic: string, payloadObj: unknown, retain = false): boolean =>
    publishRawToAll(topic, JSON.stringify(payloadObj), retain)

  const publishCurrentState = (): boolean => {
    if (closed) return false
    const msg: StateWireMessage<TState> = {
      type: 'STATE',
      src: clientId,
      state: currentState,
    }
    return publishToAll(stateTopic, msg, true)
  }

  const sendSyncMessage = (msg: SyncWireMessage): boolean => publishToAll(syncTopic, msg, false)

  const cancelSyncReply = () => {
    if (syncReplyTimer !== null) {
      globalThis.clearTimeout(syncReplyTimer)
      syncReplyTimer = null
    }
  }

  const cancelCatchupPublish = () => {
    if (catchupPublishTimer !== null) {
      globalThis.clearTimeout(catchupPublishTimer)
      catchupPublishTimer = null
    }
  }

  const scheduleSyncReply = () => {
    if (closed || syncReplyTimer !== null || !hasState(currentState)) return
    const delayMs = 60 + Math.floor(rng() * 140)
    syncReplyTimer = globalThis.setTimeout(() => {
      syncReplyTimer = null
      if (!closed && hasState(currentState)) publishCurrentState()
    }, delayMs)
  }

  const scheduleCatchupPublish = () => {
    if (closed || catchupPublishTimer !== null || !hasState(currentState)) return
    const delayMs = 40 + Math.floor(rng() * 80)
    catchupPublishTimer = globalThis.setTimeout(() => {
      catchupPublishTimer = null
      if (!closed && hasState(currentState)) publishCurrentState()
    }, delayMs)
  }

  const handleInboundMessage = (topic: string, payload: string) => {
    if (closed) return
    lastActivityAt = now()
    if (!payload) return
    const parsed = parseJsonObject(payload)
    if (!parsed) return

    const src = typeof parsed.src === 'string' ? parsed.src : ''
    if (src === clientId) return

    if (topic === stateTopic && parsed.type === 'STATE') {
      const incoming = parsed.state as unknown
      if (options.validateState) {
        if (!options.validateState(incoming, roomCode)) return
      } else if (!incoming || typeof incoming !== 'object') {
        return
      }
      const typedIncoming = incoming as TState
      if (options.isExpiredState?.(typedIncoming, now(), roomTtlMs)) return
      if (src) markPeerSeen(src)

      const merged = options.mergeState(currentState, typedIncoming)
      if (merged.changed) {
        currentState = merged.state
        saveToStorage(storage, options.storageKey, currentState)
        options.onState(currentState)
      }

      if (!merged.incomingWasBehind) {
        cancelSyncReply()
        cancelCatchupPublish()
      } else {
        scheduleCatchupPublish()
      }
      return
    }

    if (topic === syncTopic) {
      const msgRoom =
        typeof parsed.roomCode === 'string' ? normalizeRoomCode(parsed.roomCode, cfg.codeLength) : ''
      if (msgRoom !== roomCode) return

      if (parsed.type === 'SYNC_REQ') {
        if (src) markPeerSeen(src)
        sendSyncMessage({ type: 'HERE', src: clientId, roomCode, ts: now() })
        scheduleSyncReply()
      } else if (parsed.type === 'HERE') {
        if (src) markPeerSeen(src)
      } else if (parsed.type === 'BYE') {
        if (src && peerLastSeen.delete(src)) {
          emitPeerCount()
        }
      }
    }
  }

  const clearReconnectTimer = (idx: number) => {
    const t = reconnectTimers[idx]
    if (t !== null && t !== undefined) {
      globalThis.clearTimeout(t)
      reconnectTimers[idx] = null
    }
  }

  const connectBroker = (idx: number) => {
    if (closed) return
    const url = brokerUrls[idx]
    if (!url) return

    clearReconnectTimer(idx)
    const prev = clients[idx]
    if (prev) {
      clients[idx] = null
      prev.close()
    }

    connAttempt += 1
    const client = connectMqttSignal({
      brokers: [url],
      clientId: `${clientId}-b${idx}-${connAttempt.toString(36)}`,
      subscribeTopic: [stateTopic, syncTopic],
      WebSocketImpl: cfg.WebSocketImpl,
      onReady() {
        if (closed) return
        everConnected = true
        brokerFailedOnce[idx] = false
        brokerBackoffMs[idx] = cfg.reconnectBackoffMs
        lastActivityAt = now()
        emitStatus('connected')
        sendSyncMessage({ type: 'HERE', src: clientId, roomCode, ts: now() })
        if (shouldPublishInitialOnReady) {
          shouldPublishInitialOnReady = false
          publishCurrentState()
        }
        sendSyncMessage({ type: 'SYNC_REQ', src: clientId, roomCode, ts: now() })
      },
      onMessage(topic, payload) {
        handleInboundMessage(topic, payload)
      },
      onDisconnect() {
        if (closed) return
        if (clients[idx] === client) {
          clients[idx] = null
        }
        brokerFailedOnce[idx] = true
        const allFailed = brokerFailedOnce.length > 0 && brokerFailedOnce.every(Boolean)
        if (!hasReadyClient()) {
          if (allFailed && !everConnected) {
            emitStatus('error')
            options.onError?.(
              'Unable to reach sync servers. Check your connection or try disabling VPN/Wi-Fi restrictions. Retrying...',
            )
          } else {
            emitStatus(everConnected ? 'reconnecting' : 'connecting')
          }
        }
        const delay = brokerBackoffMs[idx] ?? cfg.reconnectBackoffMs
        brokerBackoffMs[idx] = Math.min(cfg.maxReconnectBackoffMs, delay * 2)
        reconnectTimers[idx] = globalThis.setTimeout(() => {
          reconnectTimers[idx] = null
          connectBroker(idx)
        }, delay)
      },
      onError() {
        if (closed) return
        brokerFailedOnce[idx] = true
        if (!hasReadyClient() && brokerFailedOnce.every(Boolean)) {
          options.onError?.(
            'Unable to reach sync servers. Check your connection or try disabling VPN/Wi-Fi restrictions. Retrying...',
          )
        }
      },
    })

    clients[idx] = client
  }

  const wakeAndResync = () => {
    if (closed) return
    const elapsed = now() - lastActivityAt
    const staleOrDisconnected = elapsed > cfg.wakeReconnectMs || !hasReadyClient()

    if (staleOrDisconnected) {
      emitStatus(everConnected ? 'reconnecting' : 'connecting')
      for (let i = 0; i < brokerUrls.length; i += 1) {
        brokerBackoffMs[i] = cfg.reconnectBackoffMs
        const c = clients[i]
        if (!c || !c.isReady() || elapsed > cfg.wakeReconnectMs) {
          connectBroker(i)
        }
      }
    } else {
      sendSyncMessage({ type: 'HERE', src: clientId, roomCode, ts: now() })
      sendSyncMessage({ type: 'SYNC_REQ', src: clientId, roomCode, ts: now() })
    }
    lastActivityAt = now()
  }

  const detachWake = attachWakeListeners(cfg.doc, cfg.win, wakeAndResync)

  if (brokerUrls.length === 0) {
    emitStatus('error')
    options.onError?.('No MQTT brokers configured')
  } else {
    emitStatus('connecting')
    for (let i = 0; i < brokerUrls.length; i += 1) {
      connectBroker(i)
    }
  }

  heartbeatTimer = globalThis.setInterval(() => {
    if (closed || !hasReadyClient()) return
    lastActivityAt = now()
    sendSyncMessage({ type: 'HERE', src: clientId, roomCode, ts: now() })
  }, heartbeatMs)

  sweepTimer = globalThis.setInterval(() => {
    if (closed) return
    const t = now()
    let removed = false
    for (const [peerId, seenAt] of peerLastSeen.entries()) {
      if (t - seenAt > peerStaleMs) {
        peerLastSeen.delete(peerId)
        removed = true
      }
    }
    if (removed) emitPeerCount()
  }, peerSweepMs)

  options.onState(currentState)

  return {
    roomCode,
    clientId,
    getState: () => currentState,
    publishState(nextState: TState) {
      if (closed) return
      const merged = options.mergeState(currentState, nextState)
      currentState = merged.state
      saveToStorage(storage, options.storageKey, currentState)
      options.onState(currentState)
      cancelSyncReply()
      cancelCatchupPublish()
      publishCurrentState()
    },
    requestSync() {
      if (closed) return
      wakeAndResync()
    },
    close(closeOpts?: { clearIfLast?: boolean }) {
      if (closed) return
      if (closeOpts?.clearIfLast && peerLastSeen.size === 0) {
        publishRawToAll(stateTopic, '', true)
        clearFromStorage(storage, options.storageKey)
      }
      sendSyncMessage({ type: 'BYE', src: clientId, roomCode })
      closed = true
      cancelSyncReply()
      cancelCatchupPublish()
      if (heartbeatTimer !== null) globalThis.clearInterval(heartbeatTimer)
      if (sweepTimer !== null) globalThis.clearInterval(sweepTimer)
      for (let i = 0; i < reconnectTimers.length; i += 1) {
        clearReconnectTimer(i)
      }
      detachWake()
      for (let i = 0; i < clients.length; i += 1) {
        const c = clients[i]
        clients[i] = null
        c?.close()
      }
      peerLastSeen.clear()
    },
  }
}
