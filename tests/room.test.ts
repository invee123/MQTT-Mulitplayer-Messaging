import assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import {
  assignRoomToTvDisplay,
  assignRoomToTvDisplayDetailed,
  brokersForRoom,
  createMqttDisplayRoom,
  createMqttGuestRoom,
  createMqttHostRoom,
  createMqttTvListener,
  isDisplayPeerId,
} from '../src/room.ts'
import { decodePackets, encodePublish, type MqttWebSocketLike } from '../src/signal.ts'
import type { DocumentLike, HostHandoffPayload, RoomError, RoomInfo, WindowLike } from '../src/types.ts'

type TestState = {
  round: number
  version?: number
  secret: number | null
  revealed: boolean
}

type TestIntent =
  | { type: 'guess'; value: number }
  | { type: 'join'; name: string }
  | { type: 'chat-sync' }

function waitFor(predicate: () => boolean, timeoutMs: number): Promise<void> {
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
      setTimeout(tick, 30)
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

function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer
}

function createMockBrokers() {
  const brokers = new Map<string, Set<MockSocket>>()
  const retained = new Map<string, Map<string, string>>()
  const connectClientIds: string[] = []

  class MockSocket implements MqttWebSocketLike {
    static CONNECTING = 0
    static OPEN = 1
    static CLOSED = 3

    readonly url: string
    binaryType = 'blob'
    readyState = MockSocket.CONNECTING
    onopen: ((event: unknown) => void) | null = null
    onmessage: ((event: { data: unknown }) => void) | null = null
    onerror: ((event: unknown) => void) | null = null
    onclose: ((event: { code?: number; reason?: string; wasClean?: boolean }) => void) | null = null
    clientId = ''
    topics = new Set<string>()

    constructor(url: string) {
      this.url = url
      let set = brokers.get(url)
      if (!set) {
        set = new Set()
        brokers.set(url, set)
      }
      set.add(this)
      queueMicrotask(() => {
        if (this.readyState !== MockSocket.CONNECTING) return
        this.readyState = MockSocket.OPEN
        this.onopen?.({})
      })
    }

    deliver(bytes: Uint8Array) {
      if (this.readyState !== MockSocket.OPEN) return
      this.onmessage?.({ data: toArrayBuffer(bytes) })
    }

    send(data: string | ArrayBufferLike | ArrayBufferView) {
      if (this.readyState !== MockSocket.OPEN) return
      const bytes =
        data instanceof ArrayBuffer
          ? new Uint8Array(data)
          : ArrayBuffer.isView(data)
            ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
            : new TextEncoder().encode(data)
      const packetType = bytes[0] >> 4

      if (packetType === 1) {
        const { next } = parseRemainingLength(bytes, 1)
        const idLen = (bytes[next + 10] << 8) | bytes[next + 11]
        this.clientId = new TextDecoder().decode(bytes.subarray(next + 12, next + 12 + idLen))
        connectClientIds.push(this.clientId)
        this.deliver(new Uint8Array([0x20, 0x02, 0x00, 0x00]))
        return
      }

      if (packetType === 8) {
        const { next } = parseRemainingLength(bytes, 1)
        const packetId = (bytes[next] << 8) | bytes[next + 1]
        const topicLen = (bytes[next + 2] << 8) | bytes[next + 3]
        const topic = new TextDecoder().decode(bytes.subarray(next + 4, next + 4 + topicLen))
        this.topics.add(topic)
        this.deliver(new Uint8Array([0x90, 0x03, packetId >> 8, packetId & 0xff, 0x00]))
        const retMap = retained.get(this.url)
        const retPayload = retMap?.get(topic)
        if (retPayload !== undefined) {
          this.deliver(encodePublish(topic, retPayload, true))
        }
        return
      }

      if (packetType === 3) {
        const { packets } = decodePackets(bytes)
        for (const pkt of packets) {
          if (pkt.type !== 'PUBLISH') continue
          if (pkt.retain) {
            let retMap = retained.get(this.url)
            if (!retMap) {
              retMap = new Map()
              retained.set(this.url, retMap)
            }
            if (pkt.payload === '') {
              retMap.delete(pkt.topic)
            } else {
              retMap.set(pkt.topic, pkt.payload)
            }
          }
          const out = encodePublish(pkt.topic, pkt.payload, Boolean(pkt.retain))
          for (const peer of brokers.get(this.url) ?? []) {
            if (peer.topics.has(pkt.topic)) peer.deliver(out)
          }
        }
      }
    }

    close() {
      if (this.readyState === MockSocket.CLOSED) return
      this.readyState = MockSocket.CLOSED
      brokers.get(this.url)?.delete(this)
      this.onclose?.({ code: 1000 })
    }
  }

  return {
    MockSocket,
    connectClientIds,
    dropUrl(url: string) {
      for (const sock of [...(brokers.get(url) ?? [])]) {
        sock.close()
      }
    },
    publishFromServer(topic: string, payload: unknown) {
      const encoded = JSON.stringify(payload)
      const frame = encodePublish(topic, encoded)
      for (const sockets of brokers.values()) {
        for (const socket of sockets) {
          if (socket.topics.has(topic)) socket.deliver(frame)
        }
      }
    },
  }
}

describe('generic mqtt room', () => {
  it('rotates brokers deterministically by room code', () => {
    const a = brokersForRoom('8TES')
    const b = brokersForRoom('8tes')
    assert.deepEqual(a, b)
    assert.equal(a.length, 2)
  })

  it('identifies display peer ids using custom or default prefix', () => {
    assert.equal(isDisplayPeerId('mmp-d-abc1234'), true)
    assert.equal(isDisplayPeerId('mmp-g-abc1234'), false)
    assert.equal(isDisplayPeerId('custom-d-1', 'custom-d-'), true)
  })

  it('delivers sanitized state, deduplicates intents, and supports kick + TV pairing', async () => {
    const mock = createMockBrokers()
    const hostStatuses: RoomInfo[] = []
    const guestStatuses: RoomInfo[] = []
    const intents: Array<{ intent: TestIntent; senderId: string }> = []
    let guestSnapshot: TestState | null = null
    let displaySnapshot: TestState | null = null

    const config = {
      topicPrefix: 'testgame/v1/room',
      peerPrefix: 'tg-',
      displayPeerPrefix: 'tg-d-',
      brokers: ['wss://mock-a.local/mqtt', 'wss://mock-b.local/mqtt'],
      WebSocketImpl: mock.MockSocket,
    }

    const host = createMqttHostRoom<TestState, TestIntent, number>(
      {
        onStatus: (info) => hostStatuses.push(info),
        onPeerChange: () => {
          host.broadcast({ round: 1, secret: 42, revealed: false }, 42)
        },
        onIntent: (intent, senderId) => intents.push({ intent, senderId }),
      },
      {
        ...config,
        initialCode: 'ABCD',
        sanitizeForPeer: (state, secret) => ({
          ...state,
          secret: state.revealed ? (secret ?? null) : null,
        }),
        sanitizeForBaseline: (state) => ({ ...state, secret: null }),
        createJoinIntent: (name) => ({ type: 'join', name }),
      },
    )

    await waitFor(() => hostStatuses.some((s) => s.status === 'connected'), 2_000)

    const guest = createMqttGuestRoom<TestState, TestIntent>(
      'ABCD',
      {
        onStatus: (info) => guestStatuses.push(info),
        onSnapshot: (state) => {
          guestSnapshot = state
        },
        getPlayerName: () => 'Alice',
      },
      config,
    )

    const display = createMqttDisplayRoom<TestState, TestIntent>(
      'ABCD',
      {
        onStatus: () => {},
        onSnapshot: (state) => {
          displaySnapshot = state
        },
      },
      config,
    )

    let assignedRoom = ''
    const tvListener = createMqttTvListener(
      'W4XK',
      {
        onAssignRoom: (roomCode) => {
          assignedRoom = roomCode
        },
      },
      config,
    )

    try {
      await waitFor(() => guestSnapshot !== null && displaySnapshot !== null, 3_000)
      assert.equal(guestSnapshot?.secret, null)
      assert.equal(displaySnapshot?.secret, null)

      guest.sendIntent({ type: 'guess', value: 77 })
      await waitFor(() => intents.some((i) => i.intent.type === 'guess'), 2_000)
      assert.equal(intents.filter((i) => i.intent.type === 'guess').length, 1)

      const paired = await assignRoomToTvDisplay('W4XK', 'ABCD', { ...config, timeoutMs: 2_000 })
      assert.equal(paired, true)
      assert.equal(assignedRoom, 'ABCD')

      const detailed = await assignRoomToTvDisplayDetailed('W4XK', 'ABCD', {
        ...config,
        timeoutMs: 2_000,
      })
      assert.equal(detailed.status, 'acknowledged')
      assert.equal(detailed.tvCode, 'W4XK')
      assert.equal(detailed.roomCode, 'ABCD')

      const guestId = guestStatuses.at(-1)?.clientId ?? ''
      host.kickPeer(guestId)
      await waitFor(() => guestStatuses.at(-1)?.status === 'error', 2_000)
    } finally {
      tvListener.close()
      display.close()
      guest.close()
      host.close()
    }
  })

  it('supports SESSION tokens, targeted ERRORs, getStateVersion gating, isolateDisplayChat, and HOST_HANDOFF', async () => {
    const mock = createMockBrokers()
    const hostStatuses: RoomInfo[] = []
    const guestStatuses: RoomInfo[] = []
    const guestErrors: RoomError[] = []
    const guestSessions: Array<{ playerId: string; playerToken: string }> = []
    const guestSnapshots: TestState[] = []
    const displayChats: unknown[] = []
    const handoffsReceived: Array<{ payload: HostHandoffPayload<TestState, unknown>; from: string }> = []
    const joinInfos: Array<{ peerId: string; token?: string }> = []

    const config = {
      topicPrefix: 'whodoneit/v1/room',
      peerPrefix: 'wd-',
      displayPeerPrefix: 'wd-d-',
      brokers: ['wss://mock-a.local/mqtt', 'wss://mock-b.local/mqtt'],
      WebSocketImpl: mock.MockSocket,
    }

    const host = createMqttHostRoom<TestState, TestIntent, number, { id: string; text: string }>(
      {
        onStatus: (info) => hostStatuses.push(info),
        onPeerChange: (peerIds) => {
          if (peerIds.length > 0) {
            host.broadcast({ round: 1, version: 2, secret: 99, revealed: false }, 99)
          }
        },
      },
      {
        ...config,
        initialCode: 'HND1',
        hostSessionId: 'sess-1',
        isolateDisplayChat: true,
        onJoin(peerId, info) {
          joinInfos.push({ peerId, token: info.playerToken })
        },
        sanitizeForPeer: (state) => ({ ...state, secret: null }),
      },
    )

    await waitFor(() => hostStatuses.some((s) => s.status === 'connected'), 2_000)

    let token = 'tok-initial'
    const guest = createMqttGuestRoom<TestState, TestIntent, number, { id: string; text: string }>(
      'HND1',
      {
        onStatus: (info) => guestStatuses.push(info),
        onSnapshot: (s) => guestSnapshots.push(s),
        onError: (err) => guestErrors.push(err),
        onSessionToken: (playerId, playerToken) => {
          token = playerToken
          guestSessions.push({ playerId, playerToken })
        },
        onHostHandoff: (payload, from) => {
          handoffsReceived.push({ payload, from })
        },
      },
      {
        ...config,
        getPlayerToken: () => token,
        getStateVersion: (s) => s.version ?? 0,
        isolateDisplayChat: true,
      },
    )

    const display = createMqttDisplayRoom<TestState, TestIntent, number, { id: string; text: string }>(
      'HND1',
      {
        onStatus: () => {},
        onChat: (msg) => displayChats.push(msg),
        onChatSync: (msgs) => displayChats.push(...msgs),
      },
      {
        ...config,
        isolateDisplayChat: true,
      },
    )

    try {
      await waitFor(() => guestSnapshots.length >= 1, 2_000)
      assert.equal(joinInfos[0]?.token, 'tok-initial')

      const guestId = guestStatuses.at(-1)!.clientId
      host.sendSession?.(guestId, 'player-1', 'tok-issued')
      await waitFor(() => guestSessions.length === 1, 2_000)
      assert.deepEqual(guestSessions[0], { playerId: 'player-1', playerToken: 'tok-issued' })

      host.sendError?.(guestId, { code: 'INVALID_MOVE', message: 'Cannot move there.', intentId: 'i-1' })
      await waitFor(() => guestErrors.length === 1, 2_000)
      assert.equal(guestErrors[0]?.code, 'INVALID_MOVE')

      // Stale stateVersion (1 <= 2) must be dropped by getStateVersion gating.
      const beforeCount = guestSnapshots.length
      host.broadcast({ round: 1, version: 1, secret: 99, revealed: false }, 99)
      await new Promise((r) => setTimeout(r, 60))
      assert.equal(guestSnapshots.length, beforeCount)

      // Display chat isolation: display does not receive chat broadcasts.
      host.broadcastChat({ id: 'c1', text: 'Secret chat' })
      await new Promise((r) => setTimeout(r, 60))
      assert.equal(displayChats.length, 0)

      // Live host handoff resolves true on HOST_HANDOFF_ACK and deduplicates across dual brokers.
      const handoffOk = await host.initiateHostHandoff?.(guestId, {
        state: { round: 2, version: 3, secret: 99, revealed: false },
        peerIds: [guestId],
        displayPeerIds: [],
      })
      assert.equal(handoffOk, true)
      assert.equal(handoffsReceived.length, 1)
      assert.equal(handoffsReceived[0]?.payload.state.round, 2)
    } finally {
      display.close()
      guest.close()
      host.close()
    }
  })

  it('recycles sockets on mobile wake (visibilitychange) and tracks dynamic host changes via HELLO', async () => {
    const mock = createMockBrokers()
    let nowMs = 10_000
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

    const hostChanges: Array<{ hostPeerId: string; hostSessionId?: string }> = []
    const guestSnapshots: TestState[] = []
    const config = {
      topicPrefix: 'wake/v1/room',
      peerPrefix: 'wk-',
      displayPeerPrefix: 'wk-d-',
      brokers: ['wss://mock-a.local/mqtt'],
      WebSocketImpl: mock.MockSocket,
      document: mockDoc,
      window: mockWin,
      now: () => nowMs,
      wakeReconnectMs: 5_000,
    }

    const host = createMqttHostRoom<TestState, TestIntent, number>(
      {
        onStatus: () => {},
        onPeerChange: () => {
          host.broadcast({ round: 1, secret: 10, revealed: false }, 10)
        },
      },
      { ...config, initialCode: 'WK11', hostSessionId: 'h-1', timing: { helloIntervalMs: 40 } },
    )

    const guest = createMqttGuestRoom<TestState, TestIntent>(
      'WK11',
      {
        onStatus: () => {},
        onSnapshot: (s) => guestSnapshots.push(s),
        onHostChanged: (hostPeerId, hostSessionId) =>
          hostChanges.push({ hostPeerId, hostSessionId }),
      },
      config,
    )

    try {
      await waitFor(() => guestSnapshots.length >= 1, 2_000)
      await waitFor(() => hostChanges.some((c) => c.hostSessionId === 'h-1'), 2_000)
      assert.equal(hostChanges.at(-1)?.hostSessionId, 'h-1')

      const connectsBeforeWake = mock.connectClientIds.length
      nowMs += 20_000
      for (const fn of docListeners.get('visibilitychange') ?? []) fn()
      await waitFor(() => mock.connectClientIds.length > connectsBeforeWake, 2_000)

      // New host session announced on lobby HELLO triggers onHostChanged.
      mock.publishFromServer('wake/v1/room/WK11/lobby', {
        type: 'HELLO',
        src: 'wk-new-host',
        hostSessionId: 'h-2',
      })
      await waitFor(() => hostChanges.some((c) => c.hostSessionId === 'h-2'), 2_000)
    } finally {
      guest.close()
      host.close()
    }
  })
})
