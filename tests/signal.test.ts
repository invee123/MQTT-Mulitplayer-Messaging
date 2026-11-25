import assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import {
  connectMqttSignal,
  decodePackets,
  encodeConnect,
  encodeDisconnect,
  encodeEmptyRetainedPublish,
  encodePingreq,
  encodePublish,
  encodeSubscribe,
  type MqttWebSocketLike,
} from '../src/signal.ts'

describe('mqtt signal codec & client', () => {
  it('encodes CONNECT with MQTT 3.1.1 header and clientId', () => {
    const pkt = encodeConnect('dial-1', 30)
    assert.equal(pkt[0], 0x10)
    assert.equal(pkt[1], 10 + 2 + 6)
    assert.equal(new TextDecoder().decode(pkt.subarray(4, 8)), 'MQTT')
    assert.equal(pkt[8], 4)
    assert.equal(pkt[9], 2)
    assert.equal(new TextDecoder().decode(pkt.subarray(14)), 'dial-1')
  })

  it('encodes and decodes PUBLISH packets including multi-byte remaining length', () => {
    const bigPayload = JSON.stringify({ data: 'a'.repeat(300) })
    const pkt = encodePublish('mmp/v1/room/K7MP/to/host', bigPayload)
    const { packets, rest } = decodePackets(pkt)
    assert.equal(rest.length, 0)
    assert.equal(packets.length, 1)
    assert.deepEqual(packets[0], {
      type: 'PUBLISH',
      topic: 'mmp/v1/room/K7MP/to/host',
      payload: bigPayload,
    })
  })

  it('encodes retained and empty retained PUBLISH packets with retain metadata', () => {
    const retained = encodePublish('mmp/v1/room/ABCD/lobby', '{"ok":true}', true)
    assert.equal(retained[0], 0x31)
    assert.deepEqual(decodePackets(retained).packets, [
      { type: 'PUBLISH', topic: 'mmp/v1/room/ABCD/lobby', payload: '{"ok":true}', retain: true },
    ])

    const cleared = encodeEmptyRetainedPublish('mmp/v1/room/ABCD/lobby')
    assert.equal(cleared[0], 0x31)
    const { packets } = decodePackets(cleared)
    assert.deepEqual(packets, [
      { type: 'PUBLISH', topic: 'mmp/v1/room/ABCD/lobby', payload: '', retain: true },
    ])
  })

  it('handles fragmented and coalesced packets in decodePackets', () => {
    const p1 = new Uint8Array([0x20, 0x02, 0x00, 0x00]) // CONNACK
    const p2 = encodePublish('t/1', 'hello')
    const combined = new Uint8Array(p1.length + p2.length)
    combined.set(p1, 0)
    combined.set(p2, p1.length)

    const firstChunk = combined.subarray(0, p1.length + 4)
    const secondChunk = combined.subarray(p1.length + 4)

    const first = decodePackets(firstChunk)
    assert.equal(first.packets.length, 1)
    assert.deepEqual(first.packets[0], { type: 'CONNACK', returnCode: 0 })
    assert.equal(first.rest.length, 4)

    const merged = new Uint8Array(first.rest.length + secondChunk.length)
    merged.set(first.rest, 0)
    merged.set(secondChunk, first.rest.length)

    const second = decodePackets(merged)
    assert.equal(second.packets.length, 1)
    assert.deepEqual(second.packets[0], { type: 'PUBLISH', topic: 't/1', payload: 'hello' })
    assert.equal(second.rest.length, 0)
  })

  it('encodes SUBSCRIBE, PINGREQ, and DISCONNECT', () => {
    const sub = encodeSubscribe(1, 'mmp/v1/room/ABCD/to/host')
    assert.equal(sub[0], 0x82)
    assert.deepEqual(encodePingreq(), new Uint8Array([0xc0, 0x00]))
    assert.deepEqual(encodeDisconnect(), new Uint8Array([0xe0, 0x00]))
  })

  it('supports WebSocketImpl injection, Blob frames, dynamic subscribe, retain meta, and DISCONNECT on close', async () => {
    const sentPackets: Uint8Array[] = []
    const received: Array<{ topic: string; payload: string; retain?: boolean }> = []
    let ready = false
    let socketInstance: MockSocket | null = null

    class MockSocket implements MqttWebSocketLike {
      binaryType = 'blob'
      readyState = 0
      onopen: ((ev: unknown) => void) | null = null
      onmessage: ((ev: { data: unknown }) => void) | null = null
      onerror: ((ev: unknown) => void) | null = null
      onclose: ((ev: { code?: number; reason?: string; wasClean?: boolean }) => void) | null = null

      constructor(_url: string) {
        socketInstance = this
        queueMicrotask(() => {
          this.readyState = 1
          this.onopen?.({})
        })
      }

      send(data: string | ArrayBufferLike | ArrayBufferView) {
        const bytes =
          data instanceof ArrayBuffer
            ? new Uint8Array(data)
            : ArrayBuffer.isView(data)
              ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
              : new TextEncoder().encode(data)
        sentPackets.push(bytes)
        const type = bytes[0] >> 4
        if (type === 1) {
          this.onmessage?.({ data: new Uint8Array([0x20, 0x02, 0x00, 0x00]).buffer })
        } else if (type === 8) {
          // Extract packetId after remaining length
          let pos = 1
          while ((bytes[pos] & 0x80) !== 0) pos += 1
          pos += 1
          const pid = (bytes[pos] << 8) | bytes[pos + 1]
          this.onmessage?.({ data: new Uint8Array([0x90, 0x03, pid >> 8, pid & 0xff, 0x00]).buffer })
        }
      }

      close() {
        this.readyState = 3
        this.onclose?.({ code: 1000 })
      }
    }

    const client = connectMqttSignal({
      brokers: ['wss://mock.local/mqtt'],
      clientId: 'sig-test-1',
      subscribeTopic: 't/initial',
      WebSocketImpl: MockSocket,
      onReady: () => {
        ready = true
      },
      onMessage: (topic, payload, meta) => {
        received.push({ topic, payload, retain: meta?.retain })
      },
      onDisconnect: () => {},
    })

    await new Promise((r) => setTimeout(r, 30))
    assert.equal(ready, true)
    assert.equal(client.isReady(), true)
    assert.equal(client.subscribe('t/dynamic'), true)

    // Deliver a retained PUBLISH inside a Blob frame
    const frame = encodePublish('t/dynamic', '{"hello":1}', true)
    const blob = new Blob([frame as Uint8Array<ArrayBuffer>])
    socketInstance!.onmessage?.({ data: blob })

    await new Promise((r) => setTimeout(r, 40))
    assert.deepEqual(received, [{ topic: 't/dynamic', payload: '{"hello":1}', retain: true }])

    client.close()
    const lastSent = sentPackets.at(-1)
    assert.deepEqual(lastSent, encodeDisconnect())
  })
})
