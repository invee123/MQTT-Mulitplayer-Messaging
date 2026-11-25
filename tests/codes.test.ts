import assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import {
  ROOM_CODE_LENGTH,
  displayUrlFor,
  generateRoomCode,
  isValidRoomCode,
  normalizeRoomCode,
  parseJoinCode,
  readDisplayModeFromUrl,
  readRoomFromUrl,
  readTvPairFromUrl,
  roomUrlFor,
  tvPairUrlFor,
} from '../src/codes.ts'

describe('room code helpers', () => {
  it('generates 4-character codes from the unambiguous alphabet', () => {
    for (let i = 0; i < 40; i += 1) {
      const code = generateRoomCode()
      assert.equal(code.length, ROOM_CODE_LENGTH)
      assert.match(code, /^[ABCDEFGHJKMNPQRSTUVWXYZ23456789]{4}$/)
    }
  })

  it('normalizes case, strips non-alphanumerics, and caps at length', () => {
    assert.equal(normalizeRoomCode('k7mp'), 'K7MP')
    assert.equal(normalizeRoomCode(' k 7-m_p '), 'K7MP')
    assert.equal(normalizeRoomCode('k7mpxyz'), 'K7MP')
    assert.equal(normalizeRoomCode('ab'), 'AB')
  })

  it('validates room codes against length and unambiguous alphabet', () => {
    assert.equal(isValidRoomCode('k7mp'), true)
    assert.equal(isValidRoomCode('AB'), false)
    assert.equal(isValidRoomCode('0O1I'), false)
    assert.equal(isValidRoomCode('K7MP1', 5), false)
    assert.equal(isValidRoomCode('K7MP2', 5), true)
  })

  it('parses direct codes and invite URLs safely', () => {
    assert.equal(parseJoinCode('k7mp'), 'K7MP')
    assert.equal(parseJoinCode('https://example.com/?room=k7mp'), 'K7MP')
    assert.equal(parseJoinCode('https://example.com/tv/k7mp'), 'K7MP')
    assert.equal(parseJoinCode('https://example.com/?tv=w4xk'), 'W4XK')
    assert.equal(parseJoinCode('https://example.com/?pair=m9pq'), 'M9PQ')
  })

  it('builds and reads room, display, and TV pairing URLs', () => {
    assert.equal(
      roomUrlFor('k7mp', 'https://example.com/tv/W4XK?display=1&tv=AAAA#hash'),
      'https://example.com/?room=K7MP',
    )
    assert.equal(
      displayUrlFor('k7mp', 'https://example.com/?tv=AAAA'),
      'https://example.com/tv?room=K7MP',
    )
    assert.equal(
      tvPairUrlFor('w4xk', 'https://example.com/tv?room=K7MP'),
      'https://example.com/?tv=W4XK',
    )
    assert.equal(readDisplayModeFromUrl('', '/tv'), true)
    assert.equal(readDisplayModeFromUrl('?room=K7MP', '/'), false)
    assert.equal(readRoomFromUrl('?room=k7mp', '/tv'), 'K7MP')
    assert.equal(readTvPairFromUrl('?tv=w4xk'), 'W4XK')
  })
})
