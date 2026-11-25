import { ROOM_CODE_LENGTH, normalizeRoomCode, randomToken } from './codes.js'
import type { DocumentLike, MqttRoomConfig, WindowLike } from './types.js'

/**
 * Default public MQTT-over-WSS brokers used as the room data plane.
 * WebRTC/STUN-only P2P fails on many phone/carrier NATs; these brokers work
 * anywhere a normal HTTPS page loads.
 */
export const DEFAULT_SIGNAL_BROKERS = [
  'wss://broker.emqx.io:8084/mqtt',
  'wss://broker.hivemq.com:8884/mqtt',
] as const

export const DEFAULT_TOPIC_PREFIX = 'mmp/v1/room'
export const DEFAULT_PEER_PREFIX = 'mmp-'
export const DEFAULT_DISPLAY_PEER_PREFIX = 'mmp-d-'

export function resolveDefaultBrokers(): readonly string[] {
  const metaEnv = (import.meta as unknown as { env?: Record<string, string | undefined> }).env
  const raw = metaEnv?.VITE_MQTT_BROKERS
  if (typeof raw === 'string' && raw.trim().length > 0) {
    const parsed = raw
      .split(',')
      .map((entry) => entry.trim())
      .filter((entry) => entry.length > 0)
    if (parsed.length > 0) return parsed
  }
  return DEFAULT_SIGNAL_BROKERS
}

/** Stable FNV-1a broker rotation so host and guests prefer the same primary endpoint. */
export function brokersForRoom(
  code: string,
  brokers: readonly string[] = resolveDefaultBrokers(),
  codeLength = ROOM_CODE_LENGTH,
): string[] {
  if (brokers.length === 0) return []
  const normalized = normalizeRoomCode(code, codeLength)
  let hash = 2166136261
  for (let i = 0; i < normalized.length; i += 1) {
    hash ^= normalized.charCodeAt(i)
    hash = Math.imul(hash, 16777619)
  }
  const start = (hash >>> 0) % brokers.length
  return [...brokers.slice(start), ...brokers.slice(0, start)]
}

export function resolveConfig(config?: MqttRoomConfig) {
  const topicPrefix = (config?.topicPrefix ?? DEFAULT_TOPIC_PREFIX).replace(/\/+$/, '')
  const peerPrefix = config?.peerPrefix ?? DEFAULT_PEER_PREFIX
  const displayPeerPrefix = config?.displayPeerPrefix ?? DEFAULT_DISPLAY_PEER_PREFIX
  const brokers = config?.brokers && config.brokers.length > 0 ? config.brokers : resolveDefaultBrokers()
  const codeLength = config?.codeLength ?? ROOM_CODE_LENGTH
  const timing = config?.timing ?? {}
  const now = config?.now ?? (() => Date.now())
  const maxPlayerNameLength = config?.maxPlayerNameLength ?? 18
  const reconnectBackoffMs = config?.reconnectBackoffMs ?? 1_500
  const maxReconnectBackoffMs = config?.maxReconnectBackoffMs ?? 30_000
  const wakeReconnectMs = config?.wakeReconnectMs ?? 12_000

  const doc: DocumentLike | null =
    config?.document !== undefined
      ? config.document
      : typeof globalThis.document !== 'undefined'
        ? (globalThis.document as unknown as DocumentLike)
        : null

  const win: WindowLike | null =
    config?.window !== undefined
      ? config.window
      : typeof globalThis.window !== 'undefined'
        ? (globalThis.window as unknown as WindowLike)
        : null

  return {
    topicPrefix,
    peerPrefix,
    displayPeerPrefix,
    brokers,
    codeLength,
    timing,
    WebSocketImpl: config?.WebSocketImpl,
    doc,
    win,
    now,
    maxPlayerNameLength,
    reconnectBackoffMs,
    maxReconnectBackoffMs,
    wakeReconnectMs,
  }
}

export function createMessageId(): string {
  if (typeof globalThis.crypto !== 'undefined' && typeof globalThis.crypto.randomUUID === 'function') {
    return globalThis.crypto.randomUUID()
  }
  return `msg-${Date.now().toString(36)}-${randomToken(8)}`
}

export function attachWakeListeners(
  doc: DocumentLike | null,
  win: WindowLike | null,
  onWake: () => void,
): () => void {
  const handleVisibility = () => {
    if (doc?.visibilityState === 'visible') {
      onWake()
    }
  }
  const handleOnlineOrShow = () => {
    onWake()
  }

  if (doc) doc.addEventListener('visibilitychange', handleVisibility)
  if (win) {
    win.addEventListener('online', handleOnlineOrShow)
    win.addEventListener('pageshow', handleOnlineOrShow)
  }

  return () => {
    if (doc) doc.removeEventListener('visibilitychange', handleVisibility)
    if (win) {
      win.removeEventListener('online', handleOnlineOrShow)
      win.removeEventListener('pageshow', handleOnlineOrShow)
    }
  }
}
