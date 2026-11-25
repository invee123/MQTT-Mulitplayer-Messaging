import assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import { createMqttPeerRoom, roomStateTopic, type PeerRoomConnectionStatus } from '../src/peerRoom.ts'
import { decodePackets, encodePublish, type MqttWebSocketLike } from '../src/signal.ts'
import type { DocumentLike, StorageLike, WindowLike } from '../src/types.ts'

type PeerTestState = {
  roomCode: string
  epoch: number
  seq: number
  items: string[]
  updatedAt: number
}

function createEmpty(code: string): PeerTestState {
  return { roomCode: code, epoch: 1, seq: 0, items: [], updatedAt: 0 }
}

function mergePeerState(local: PeerTestState, incoming: PeerTestState) {
  if (incoming.epoch > local.epoch) {
    return { state: incoming, changed: true, incomingWasBehind: false }
  }
  if (incoming.epoch < local.epoch) {
    return { state: local, changed: false, incomingWasBehind: true }
  }
  const mergedItems = Array.from(new Set([...local.items, ...incoming.items])).sort()
  const winner = incoming.seq >= local.seq ? incoming : local
  const next: PeerTestState = {
    roomCode: local.roomCode,
    epoch: local.epoch,
    seq: winner.seq,
    items: mergedItems,
    updatedAt: Math.max(local.updatedAt, incoming.updatedAt),
  }
  const changed =
    next.seq !== local.seq ||
    next.items.length !== local.items.length ||
    next.updatedAt !== local.updatedAt
  const incomingWasBehind =
    next.seq !== incoming.seq || next.items.length !== incoming.items.length
  return { state: next, changed, incomingWasBehind }
}

function createMemoryStorage(): StorageLike {
  const map = new Map<string, string>()
  return {
    getItem: (k) => map.get(k) ?? null,
    setItem: (k, v) => {
      map.set(k, v)
    },
    removeItem: (k) => {
      map.delete(k)
    },
  }
}

function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const start = Date.now()
  return new Promise((resolve, reject) => {
    const tick = () => {
      if (predicate()) {
        resolve()
        return
      }
      if (Date.now() - start > timeoutMs) {
        reject(new Error(`Timeout after ${timeoutMs}ms`))
        return
      }
      setTimeout(tick, 20)
    }
    tick()
  })
}

function parseRemainingLength(bytes: Uint8Array, start: number): { length: number; next: number } {
  let multiplier = 1
  let length = 0
  let pos = start
  while (pos < bytes.length) {
    const byte = bytes[pos++]
    length += (byte & 0x7f) * multiplier
    if ((byte & 0x80) === 0) return { length, next: pos }
    multiplier *= 128
  }
  return { length, next: pos }
}

function createPeerMockBrokers(failUrls: Set<string> = new Set()) {
  const brokers = new Map<string, Set<MockSocket>>()
  const retained = new Map<string, Map<string, string>>()
  const connectClientIds: string[] = []
  const publishedRecords: Array<{ url: string; topic: string; payload: string; retain: boolean }> = []

  class MockSocket implements MqttWebSocketLike {
    binaryType = 'blob'
    readyState = 0
    onopen: ((ev: unknown) => void) | null = null
    onmessage: ((ev: { data: unknown }) => void) | null = null
    onerror: ((ev: unknown) => void) | null = null
    onclose: ((ev: { code?: number }) => void) | null = null
    clientId = ''
    topics = new Set<string>()

    constructor(public readonly url: string) {
      let set = brokers.get(url)
      if (!set) {
        set = new Set()
        brokers.set(url, set)
      }
      set.add(this)
      queueMicrotask(() => {
        if (failUrls.has(url)) {
          this.readyState = 3
          this.onerror?.({})
          this.onclose?.({ code: 1006 })
          return
        }
        if (this.readyState !== 0) return
        this.readyState = 1
        this.onopen?.({})
      })
    }

    deliver(bytes: Uint8Array) {
      if (this.readyState !== 1) return
      this.onmessage?.({
        data: bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength),
      })
    }

    send(data: string | ArrayBufferLike | ArrayBufferView) {
      if (this.readyState !== 1) return
      const bytes =
        data instanceof ArrayBuffer
          ? new Uint8Array(data)
          : ArrayBuffer.isView(data)
            ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
            : new TextEncoder().encode(data)
      const type = bytes[0] >> 4

      if (type === 1) {
        const { next } = parseRemainingLength(bytes, 1)
        const idLen = (bytes[next + 10] << 8) | bytes[next + 11]
        this.clientId = new TextDecoder().decode(bytes.subarray(next + 12, next + 12 + idLen))
        connectClientIds.push(this.clientId)
        this.deliver(new Uint8Array([0x20, 0x02, 0x00, 0x00]))
        return
      }
      if (type === 8) {
        const { next } = parseRemainingLength(bytes, 1)
        const pid = (bytes[next] << 8) | bytes[next + 1]
        const topicLen = (bytes[next + 2] << 8) | bytes[next + 3]
        const topic = new TextDecoder().decode(bytes.subarray(next + 4, next + 4 + topicLen))
        this.topics.add(topic)
        this.deliver(new Uint8Array([0x90, 0x03, pid >> 8, pid & 0xff, 0x00]))
        const ret = retained.get(this.url)?.get(topic)
        if (ret !== undefined && ret !== '') {
          this.deliver(encodePublish(topic, ret, true))
        }
        return
      }
      if (type === 3) {
        const { packets } = decodePackets(bytes)
        for (const pkt of packets) {
          if (pkt.type !== 'PUBLISH') continue
          publishedRecords.push({
            url: this.url,
            topic: pkt.topic,
            payload: pkt.payload,
            retain: Boolean(pkt.retain),
          })
          if (pkt.retain) {
            let map = retained.get(this.url)
            if (!map) {
              map = new Map()
              retained.set(this.url, map)
            }
            if (pkt.payload === '') map.delete(pkt.topic)
            else map.set(pkt.topic, pkt.payload)
          }
          const frame = encodePublish(pkt.topic, pkt.payload, Boolean(pkt.retain))
          for (const peer of brokers.get(this.url) ?? []) {
            if (peer.topics.has(pkt.topic)) peer.deliver(frame)
          }
        }
      }
    }

    close() {
      if (this.readyState === 3) return
      this.readyState = 3
      brokers.get(this.url)?.delete(this)
      this.onclose?.({ code: 1000 })
    }
  }

  return {
    MockSocket,
    retained,
    connectClientIds,
    publishedRecords,
  }
}

describe('createMqttPeerRoom (hostless CRDT peer room)', () => {
  it('merges concurrent edits via CRDT, tracks presence, and heals lagging peers', async () => {
    const mock = createPeerMockBrokers()
    const brokers = ['wss://peer-a.local/mqtt', 'wss://peer-b.local/mqtt']
    const statusesA: PeerRoomConnectionStatus[] = []
    let countA = 1
    let stateA: PeerTestState = createEmpty('CR12')
    let stateB: PeerTestState = createEmpty('CR12')

    const roomA = createMqttPeerRoom<PeerTestState>({
      roomCode: 'CR12',
      clientId: 'peer-a',
      topicPrefix: 'cranium/v1/room',
      brokers,
      WebSocketImpl: mock.MockSocket,
      storage: createMemoryStorage(),
      storageKey: 'cr:CR12:a',
      initialState: { roomCode: 'CR12', epoch: 1, seq: 1, items: ['card-1'], updatedAt: 1000 },
      publishInitialState: true,
      createEmptyState: createEmpty,
      mergeState: mergePeerState,
      hasState: (s) => s.updatedAt > 0,
      onState: (s) => {
        stateA = s
      },
      onStatus: (s) => statusesA.push(s),
      onPeerCount: (c) => {
        countA = c
      },
    })

    await waitFor(() => statusesA.includes('connected'))

    const roomB = createMqttPeerRoom<PeerTestState>({
      roomCode: 'CR12',
      clientId: 'peer-b',
      topicPrefix: 'cranium/v1/room',
      brokers,
      WebSocketImpl: mock.MockSocket,
      storage: createMemoryStorage(),
      storageKey: 'cr:CR12:b',
      createEmptyState: createEmpty,
      mergeState: mergePeerState,
      hasState: (s) => s.updatedAt > 0,
      onState: (s) => {
        stateB = s
      },
      onStatus: () => {},
      onPeerCount: () => {},
    })

    try {
      // Peer B receives retained state from Peer A immediately on subscribe.
      await waitFor(() => stateB.items.includes('card-1'))
      await waitFor(() => countA === 2)

      // Concurrent draws on A and B both survive via set union.
      roomA.publishState({ ...roomA.getState(), seq: 2, items: ['card-1', 'card-a'], updatedAt: 2000 })
      roomB.publishState({ ...roomB.getState(), seq: 2, items: ['card-1', 'card-b'], updatedAt: 2001 })

      await waitFor(
        () =>
          stateA.items.includes('card-a') &&
          stateA.items.includes('card-b') &&
          stateB.items.includes('card-a') &&
          stateB.items.includes('card-b'),
      )

      // Peer B closes normally; Peer A sees BYE and drops count back to 1.
      roomB.close()
      await waitFor(() => countA === 1)
    } finally {
      roomB.close()
      roomA.close({ clearIfLast: true })
    }

    // Last peer closing with clearIfLast deletes retained broker state.
    const stateTopic = roomStateTopic('CR12', 'cranium/v1/room')
    assert.equal(mock.retained.get(brokers[0])?.has(stateTopic), false)
  })

  it('tolerates single-broker failure silently, rejects expired state, and recycles sockets on wake', async () => {
    const mock = createPeerMockBrokers(new Set(['wss://dead.local/mqtt']))
    let nowMs = 50_000
    const docListeners = new Map<string, Set<() => void>>()
    const winListeners = new Map<string, Set<() => void>>()
    const mockDoc: DocumentLike = {
      visibilityState: 'visible',
      addEventListener(type, fn) {
        let s = docListeners.get(type)
        if (!s) {
          s = new Set()
          docListeners.set(type, s)
        }
        s.add(fn)
      },
      removeEventListener(type, fn) {
        docListeners.get(type)?.delete(fn)
      },
    }
    const mockWin: WindowLike = {
      addEventListener(type, fn) {
        let s = winListeners.get(type)
        if (!s) {
          s = new Set()
          winListeners.set(type, s)
        }
        s.add(fn)
      },
      removeEventListener(type, fn) {
        winListeners.get(type)?.delete(fn)
      },
    }

    const errors: string[] = []
    const statuses: PeerRoomConnectionStatus[] = []
    const storage = createMemoryStorage()
    const storageKey = 'cr:WAKE'
    storage.setItem(
      storageKey,
      JSON.stringify({ roomCode: 'WAKE', epoch: 1, seq: 9, items: ['old'], updatedAt: 1000 }),
    )

    const room = createMqttPeerRoom<PeerTestState>({
      roomCode: 'WAKE',
      clientId: 'wake-peer',
      topicPrefix: 'cranium/v1/room',
      brokers: ['wss://dead.local/mqtt', 'wss://alive.local/mqtt'],
      WebSocketImpl: mock.MockSocket,
      document: mockDoc,
      window: mockWin,
      now: () => nowMs,
      wakeReconnectMs: 5_000,
      roomTtlMs: 10_000,
      storage,
      storageKey,
      createEmptyState: createEmpty,
      mergeState: mergePeerState,
      isExpiredState: (s, t, ttl) => s.updatedAt > 0 && t - s.updatedAt > ttl,
      hasState: (s) => s.updatedAt > 0,
      onState: () => {},
      onStatus: (s) => statuses.push(s),
      onPeerCount: () => {},
      onError: (msg) => errors.push(msg),
    })

    try {
      await waitFor(() => statuses.includes('connected'))
      // Expired localStorage state was ignored and purged.
      assert.equal(room.getState().seq, 0)
      assert.equal(storage.getItem(storageKey), null)
      // Single dead broker did not trigger an error because alive.local succeeded.
      assert.equal(errors.length, 0)

      const connectsBefore = mock.connectClientIds.length
      nowMs += 20_000
      for (const fn of docListeners.get('visibilitychange') ?? []) fn()
      await waitFor(() => mock.connectClientIds.length > connectsBefore)
    } finally {
      room.close()
    }
  })
})
