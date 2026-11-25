/**
 * Minimal zero-dependency MQTT 3.1.1 over WebSocket client.
 * Supports CONNECT, CONNACK, SUBSCRIBE, SUBACK, PUBLISH (QoS 0, retain), PINGREQ, and DISCONNECT.
 */

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()
const WS_OPEN = 1

export type MqttPacket =
  | { type: 'CONNACK'; returnCode: number }
  | { type: 'SUBACK'; packetId: number }
  | { type: 'PUBLISH'; topic: string; payload: string; retain?: boolean }
  | { type: 'PINGRESP' }
  | { type: 'OTHER'; packetType: number }

export type MqttWebSocketLike = {
  binaryType: string
  readonly readyState: number
  send: (data: string | ArrayBufferLike | ArrayBufferView) => void
  close: (code?: number, reason?: string) => void
  onopen: ((ev: unknown) => void) | null
  onmessage: ((ev: { data: unknown }) => void) | null
  onerror: ((ev: unknown) => void) | null
  onclose: ((ev: { code?: number; reason?: string; wasClean?: boolean }) => void) | null
}

export type MqttWebSocketConstructor = new (
  url: string,
  protocols?: string | string[],
) => MqttWebSocketLike

function encodeRemainingLength(len: number): number[] {
  const out: number[] = []
  let x = len
  do {
    let digit = x % 128
    x = Math.floor(x / 128)
    if (x > 0) digit |= 0x80
    out.push(digit)
  } while (x > 0)
  return out
}

function encodeUtf8String(str: string): Uint8Array {
  const bytes = textEncoder.encode(str)
  const out = new Uint8Array(2 + bytes.length)
  out[0] = (bytes.length >> 8) & 0xff
  out[1] = bytes.length & 0xff
  out.set(bytes, 2)
  return out
}

function buildPacket(headerByte: number, body: Uint8Array): Uint8Array {
  const rem = encodeRemainingLength(body.length)
  const pkt = new Uint8Array(1 + rem.length + body.length)
  pkt[0] = headerByte
  pkt.set(rem, 1)
  pkt.set(body, 1 + rem.length)
  return pkt
}

export function encodeConnect(clientId: string, keepAliveSec = 30): Uint8Array {
  const id = encodeUtf8String(clientId)
  const body = new Uint8Array(10 + id.length)
  // Protocol Name: "MQTT"
  body[0] = 0
  body[1] = 4
  body[2] = 77
  body[3] = 81
  body[4] = 84
  body[5] = 84
  body[6] = 4 // Protocol Level 4 (MQTT 3.1.1)
  body[7] = 2 // Clean Session
  body[8] = (keepAliveSec >> 8) & 0xff
  body[9] = keepAliveSec & 0xff
  body.set(id, 10)
  return buildPacket(0x10, body)
}

export function encodeSubscribe(packetId: number, topic: string): Uint8Array {
  const topicBytes = encodeUtf8String(topic)
  const body = new Uint8Array(2 + topicBytes.length + 1)
  body[0] = (packetId >> 8) & 0xff
  body[1] = packetId & 0xff
  body.set(topicBytes, 2)
  body[2 + topicBytes.length] = 0 // QoS 0
  return buildPacket(0x82, body)
}

export function encodePublish(topic: string, payload: string, retain = false): Uint8Array {
  const topicBytes = encodeUtf8String(topic)
  const payloadBytes = textEncoder.encode(payload)
  const body = new Uint8Array(topicBytes.length + payloadBytes.length)
  body.set(topicBytes, 0)
  body.set(payloadBytes, topicBytes.length)
  return buildPacket(retain ? 0x31 : 0x30, body)
}

/** Empty retained payload — MQTT 3.1.1 treats this as "delete the retained message". */
export function encodeEmptyRetainedPublish(topic: string): Uint8Array {
  return encodePublish(topic, '', true)
}

export function encodePingreq(): Uint8Array {
  return new Uint8Array([0xc0, 0x00])
}

export function encodeDisconnect(): Uint8Array {
  return new Uint8Array([0xe0, 0x00])
}

export function decodePackets(buffer: Uint8Array): { packets: MqttPacket[]; rest: Uint8Array } {
  const packets: MqttPacket[] = []
  let offset = 0

  while (offset < buffer.length) {
    if (offset + 2 > buffer.length) break

    const header = buffer[offset]
    let multiplier = 1
    let remLen = 0
    let pos = offset + 1
    let completeHeader = false

    while (pos < buffer.length) {
      const byte = buffer[pos++]
      remLen += (byte & 0x7f) * multiplier
      if ((byte & 0x80) === 0) {
        completeHeader = true
        break
      }
      multiplier *= 128
      if (multiplier > 128 * 128 * 128) {
        // Malformed remaining length; discard buffer.
        return { packets, rest: new Uint8Array(0) }
      }
    }

    if (!completeHeader || pos + remLen > buffer.length) break

    const body = buffer.subarray(pos, pos + remLen)
    const packetType = header >> 4
    offset = pos + remLen

    if (packetType === 2 && body.length >= 2) {
      packets.push({ type: 'CONNACK', returnCode: body[1] })
    } else if (packetType === 9 && body.length >= 2) {
      packets.push({ type: 'SUBACK', packetId: (body[0] << 8) | body[1] })
    } else if (packetType === 3 && body.length >= 2) {
      const topicLen = (body[0] << 8) | body[1]
      const qos = (header >> 1) & 0x03
      const retain = (header & 0x01) !== 0
      const payloadStart = 2 + topicLen + (qos > 0 ? 2 : 0)
      if (payloadStart <= body.length) {
        const topic = textDecoder.decode(body.subarray(2, 2 + topicLen))
        const payload = textDecoder.decode(body.subarray(payloadStart))
        packets.push({
          type: 'PUBLISH',
          topic,
          payload,
          ...(retain ? { retain: true } : {}),
        })
      }
    } else if (packetType === 13) {
      packets.push({ type: 'PINGRESP' })
    } else {
      packets.push({ type: 'OTHER', packetType })
    }
  }

  return { packets, rest: buffer.slice(offset) }
}

function toUint8ArraySync(data: unknown): Uint8Array | null {
  if (data instanceof ArrayBuffer) return new Uint8Array(data)
  if (ArrayBuffer.isView(data)) {
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
  }
  return null
}

export type MqttSignalClient = {
  publish: (topic: string, data: unknown, retain?: boolean) => boolean
  publishRaw: (topic: string, payload: string, retain?: boolean) => boolean
  /** Clears the retained message for the topic (used for explicit baseline cleanup). */
  deleteRetained: (topic: string) => boolean
  /** Subscribe to an additional topic after CONNACK (QoS 0). */
  subscribe: (topic: string) => boolean
  isReady: () => boolean
  isOpen: () => boolean
  close: () => void
}

export function connectMqttSignal(options: {
  brokers: readonly string[]
  clientId: string
  /** One or more topics to subscribe after CONNACK (QoS 0). */
  subscribeTopic: string | readonly string[]
  keepAliveSec?: number
  pingIntervalMs?: number
  WebSocketImpl?: MqttWebSocketConstructor
  onReady: () => void
  onMessage: (topic: string, payload: string, meta?: { retain: boolean }) => void
  onDisconnect: (hadConnected: boolean) => void
  onError?: (error: Error) => void
}): MqttSignalClient {
  let brokerIndex = 0
  let ws: MqttWebSocketLike | null = null
  let pingTimer: ReturnType<typeof globalThis.setInterval> | null = null
  let closed = false
  let ready = false
  let connackReceived = false
  let nextPacketId = 1
  const pendingSubIds = new Set<number>()
  let rxBuffer: Uint8Array<ArrayBufferLike> = new Uint8Array(0)
  let blobChain: Promise<void> = Promise.resolve()

  const keepAliveSec = options.keepAliveSec ?? 30
  const pingIntervalMs = options.pingIntervalMs ?? 22_000
  const topics = (
    typeof options.subscribeTopic === 'string' ? [options.subscribeTopic] : [...options.subscribeTopic]
  ).filter(Boolean)

  const WsCtor =
    options.WebSocketImpl ??
    (typeof globalThis.WebSocket !== 'undefined'
      ? (globalThis.WebSocket as unknown as MqttWebSocketConstructor)
      : undefined)

  const allocatePacketId = (): number => {
    const id = nextPacketId++
    if (nextPacketId > 0xffff) nextPacketId = 1
    return id
  }

  const clearPing = () => {
    if (pingTimer !== null) {
      globalThis.clearInterval(pingTimer)
      pingTimer = null
    }
  }

  const sendBytes = (bytes: Uint8Array): boolean => {
    if (!ws || ws.readyState !== WS_OPEN) return false
    try {
      ws.send(bytes as Uint8Array<ArrayBuffer>)
      return true
    } catch (err) {
      options.onError?.(err instanceof Error ? err : new Error(String(err)))
      return false
    }
  }

  const markReady = (connectedThisTry: { value: boolean }) => {
    if (connectedThisTry.value) return
    connectedThisTry.value = true
    ready = true
    clearPing()
    if (pingIntervalMs > 0) {
      pingTimer = globalThis.setInterval(() => {
        if (!closed && connackReceived) {
          sendBytes(encodePingreq())
        }
      }, pingIntervalMs)
    }
    options.onReady()
  }

  const connectNext = () => {
    if (closed || brokerIndex >= options.brokers.length) {
      if (!closed) options.onDisconnect(ready)
      return
    }

    if (!WsCtor) {
      options.onError?.(new Error('WebSocket implementation is unavailable in this environment'))
      options.onDisconnect(false)
      return
    }

    const url = options.brokers[brokerIndex++]
    const connectedThisTry = { value: false }
    connackReceived = false
    pendingSubIds.clear()
    rxBuffer = new Uint8Array(0)

    let socket: MqttWebSocketLike
    try {
      socket = new WsCtor(url, 'mqtt')
      socket.binaryType = 'arraybuffer'
    } catch (err) {
      options.onError?.(err instanceof Error ? err : new Error(String(err)))
      if (brokerIndex < options.brokers.length) {
        connectNext()
      } else {
        options.onDisconnect(false)
      }
      return
    }
    ws = socket

    const processChunk = (incoming: Uint8Array) => {
      if (closed || ws !== socket || incoming.length === 0) return
      const merged = new Uint8Array(rxBuffer.length + incoming.length)
      merged.set(rxBuffer, 0)
      merged.set(incoming, rxBuffer.length)
      const { packets, rest } = decodePackets(merged)
      rxBuffer = rest

      for (const pkt of packets) {
        if (closed || ws !== socket) return
        if (pkt.type === 'CONNACK') {
          if (pkt.returnCode !== 0) {
            options.onError?.(new Error(`MQTT CONNACK refused with code ${pkt.returnCode}`))
            socket.close()
            return
          }
          connackReceived = true
          if (topics.length === 0) {
            markReady(connectedThisTry)
            continue
          }
          for (const topic of topics) {
            const id = allocatePacketId()
            pendingSubIds.add(id)
            sendBytes(encodeSubscribe(id, topic))
          }
        } else if (pkt.type === 'SUBACK') {
          pendingSubIds.delete(pkt.packetId)
          if (connackReceived && pendingSubIds.size === 0) {
            markReady(connectedThisTry)
          }
        } else if (pkt.type === 'PUBLISH') {
          options.onMessage(pkt.topic, pkt.payload, { retain: Boolean(pkt.retain) })
        }
      }
    }

    socket.onopen = () => {
      if (closed || ws !== socket) return
      sendBytes(encodeConnect(options.clientId, keepAliveSec))
    }

    socket.onmessage = (event) => {
      if (closed || ws !== socket) return
      const syncBytes = toUint8ArraySync(event.data)
      if (syncBytes) {
        processChunk(syncBytes)
        return
      }
      if (typeof Blob !== 'undefined' && event.data instanceof Blob) {
        const blob = event.data
        blobChain = blobChain
          .then(() => blob.arrayBuffer())
          .then((buf) => {
            processChunk(new Uint8Array(buf))
          })
          .catch((err) => {
            options.onError?.(err instanceof Error ? err : new Error(String(err)))
          })
      }
    }

    socket.onerror = () => {
      if (closed || ws !== socket) return
      options.onError?.(new Error(`MQTT WebSocket error on ${url}`))
      // onclose handles fallback or disconnect notification.
    }

    socket.onclose = () => {
      if (ws === socket) ws = null
      connackReceived = false
      clearPing()
      if (closed) return
      if (!connectedThisTry.value && brokerIndex < options.brokers.length) {
        connectNext()
        return
      }
      ready = false
      options.onDisconnect(connectedThisTry.value)
    }
  }

  connectNext()

  return {
    publish(topic: string, data: unknown, retain = false) {
      if (closed || !connackReceived || !ws || ws.readyState !== WS_OPEN) return false
      return sendBytes(encodePublish(topic, JSON.stringify(data), retain))
    },
    publishRaw(topic: string, payload: string, retain = false) {
      if (closed || !connackReceived || !ws || ws.readyState !== WS_OPEN) return false
      return sendBytes(encodePublish(topic, payload, retain))
    },
    deleteRetained(topic: string) {
      if (closed || !connackReceived || !ws || ws.readyState !== WS_OPEN) return false
      return sendBytes(encodeEmptyRetainedPublish(topic))
    },
    subscribe(topic: string) {
      if (closed || !connackReceived || !ws || ws.readyState !== WS_OPEN || !topic) return false
      const id = allocatePacketId()
      pendingSubIds.add(id)
      return sendBytes(encodeSubscribe(id, topic))
    },
    isReady() {
      return !closed && ready && Boolean(ws && ws.readyState === WS_OPEN)
    },
    isOpen() {
      return !closed && connackReceived && Boolean(ws && ws.readyState === WS_OPEN)
    },
    close() {
      if (closed) return
      closed = true
      ready = false
      connackReceived = false
      clearPing()
      if (ws && ws.readyState === WS_OPEN) {
        try {
          ws.send(encodeDisconnect() as Uint8Array<ArrayBuffer>)
        } catch {
          // Ignore teardown send errors.
        }
      }
      try {
        ws?.close(1000, 'client closed')
      } catch {
        // Ignore close errors.
      }
      ws = null
    },
  }
}
