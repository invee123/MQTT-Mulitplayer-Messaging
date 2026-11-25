import assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import {
  createWakeLockController,
  type WakeLockDocumentLike,
  type WakeLockNavigatorLike,
  type WakeLockSentinelLike,
} from '../src/wakeLock.ts'

describe('createWakeLockController', () => {
  it('reports unsupported when Wake Lock API is missing', async () => {
    const ctrl = createWakeLockController({ navigator: {}, document: null })
    assert.equal(ctrl.getState().supported, false)
    assert.equal(ctrl.getState().status, 'unsupported')
    assert.equal(await ctrl.acquire(), false)
    ctrl.destroy()
  })

  it('acquires, releases, and re-acquires on visibilitychange', async () => {
    let requestCount = 0
    let released = false
    let releaseListener: (() => void) | null = null
    let visListener: (() => void) | null = null

    const nav: WakeLockNavigatorLike = {
      wakeLock: {
        async request() {
          requestCount += 1
          released = false
          const sentinel: WakeLockSentinelLike = {
            get released() {
              return released
            },
            async release() {
              released = true
              releaseListener?.()
            },
            addEventListener(_type, fn) {
              releaseListener = fn
            },
            removeEventListener() {
              releaseListener = null
            },
          }
          return sentinel
        },
      },
    }

    const doc: WakeLockDocumentLike = {
      visibilityState: 'visible',
      addEventListener(_type, fn) {
        visListener = fn
      },
      removeEventListener() {
        visListener = null
      },
    }

    const ctrl = createWakeLockController({ navigator: nav, document: doc })
    assert.equal(await ctrl.acquire(), true)
    assert.equal(ctrl.getState().status, 'active')
    assert.equal(ctrl.getState().active, true)
    assert.equal(requestCount, 1)

    // Simulate browser releasing sentinel when backgrounded, then returning visible.
    released = true
    releaseListener?.()
    assert.equal(ctrl.getState().status, 'released')

    visListener?.()
    await new Promise((r) => setTimeout(r, 20))
    assert.equal(requestCount, 2)
    assert.equal(ctrl.getState().status, 'active')

    await ctrl.release()
    assert.equal(ctrl.getState().status, 'idle')
    assert.equal(ctrl.getState().active, false)

    ctrl.destroy()
  })

  it('reports denied when navigator.wakeLock.request rejects', async () => {
    const nav: WakeLockNavigatorLike = {
      wakeLock: {
        async request() {
          throw new Error('Battery saver active')
        },
      },
    }
    const ctrl = createWakeLockController({ navigator: nav, document: null })
    assert.equal(await ctrl.acquire(), false)
    assert.equal(ctrl.getState().status, 'denied')
    assert.equal(ctrl.getState().errorMessage, 'Battery saver active')
    ctrl.destroy()
  })
})
