export type WakeLockStatus = 'idle' | 'active' | 'unsupported' | 'denied' | 'released'

export type WakeLockState = {
  status: WakeLockStatus
  active: boolean
  supported: boolean
  errorMessage: string | null
}

export type WakeLockSentinelLike = {
  released?: boolean
  release: () => Promise<void>
  addEventListener?: (type: 'release', listener: () => void) => void
  removeEventListener?: (type: 'release', listener: () => void) => void
}

export type WakeLockNavigatorLike = {
  wakeLock?: {
    request: (type: 'screen') => Promise<WakeLockSentinelLike>
  }
}

export type WakeLockDocumentLike = {
  readonly visibilityState: string
  addEventListener: (type: 'visibilitychange', listener: () => void) => void
  removeEventListener: (type: 'visibilitychange', listener: () => void) => void
}

export type CreateWakeLockControllerOptions = {
  navigator?: WakeLockNavigatorLike | null
  document?: WakeLockDocumentLike | null
  onStatusChange?: (state: WakeLockState) => void
}

export type WakeLockController = {
  acquire: () => Promise<boolean>
  retry: () => Promise<boolean>
  release: () => Promise<void>
  destroy: () => void
  getState: () => WakeLockState
  subscribe: (listener: (state: WakeLockState) => void) => () => void
}

/**
 * Manages the browser Screen Wake Lock API and automatically re-acquires
 * the lock when the tab returns to visibility.
 */
export function createWakeLockController(
  options: CreateWakeLockControllerOptions = {},
): WakeLockController {
  const nav =
    options.navigator !== undefined
      ? options.navigator
      : typeof globalThis.navigator !== 'undefined'
        ? (globalThis.navigator as unknown as WakeLockNavigatorLike)
        : null

  const doc =
    options.document !== undefined
      ? options.document
      : typeof globalThis.document !== 'undefined'
        ? (globalThis.document as unknown as WakeLockDocumentLike)
        : null

  const supported = Boolean(nav?.wakeLock && typeof nav.wakeLock.request === 'function')

  let state: WakeLockState = {
    status: supported ? 'idle' : 'unsupported',
    active: false,
    supported,
    errorMessage: null,
  }

  let sentinel: WakeLockSentinelLike | null = null
  let wantsLock = false
  let destroyed = false
  let inFlightAcquire: Promise<boolean> | null = null
  const listeners = new Set<(next: WakeLockState) => void>()

  const notify = (next: WakeLockState): void => {
    state = next
    const snapshot = { ...state }
    options.onStatusChange?.(snapshot)
    for (const listener of listeners) {
      listener(snapshot)
    }
  }

  const handleSentinelRelease = (): void => {
    sentinel = null
    if (destroyed) return
    notify({
      status: wantsLock ? 'released' : 'idle',
      active: false,
      supported,
      errorMessage: null,
    })
  }

  const detachSentinelListeners = (target: WakeLockSentinelLike | null): void => {
    if (!target) return
    target.removeEventListener?.('release', handleSentinelRelease)
  }

  const acquire = async (): Promise<boolean> => {
    if (destroyed) return false
    wantsLock = true

    if (!supported || !nav?.wakeLock) {
      if (state.status !== 'unsupported') {
        notify({
          status: 'unsupported',
          active: false,
          supported: false,
          errorMessage: null,
        })
      }
      return false
    }

    if (sentinel && !sentinel.released) {
      return true
    }

    if (inFlightAcquire) {
      return inFlightAcquire
    }

    inFlightAcquire = (async () => {
      try {
        const nextSentinel = await nav.wakeLock!.request('screen')
        if (destroyed || !wantsLock) {
          await nextSentinel.release().catch(() => {})
          return false
        }
        detachSentinelListeners(sentinel)
        sentinel = nextSentinel
        nextSentinel.addEventListener?.('release', handleSentinelRelease)
        notify({
          status: 'active',
          active: true,
          supported: true,
          errorMessage: null,
        })
        return true
      } catch (err) {
        if (!destroyed) {
          notify({
            status: 'denied',
            active: false,
            supported: true,
            errorMessage: err instanceof Error ? err.message : String(err),
          })
        }
        return false
      } finally {
        inFlightAcquire = null
      }
    })()

    return inFlightAcquire
  }

  const handleVisibilityChange = (): void => {
    if (destroyed || !wantsLock || !doc) return
    if (doc.visibilityState === 'visible') {
      void acquire()
    }
  }

  if (doc) {
    doc.addEventListener('visibilitychange', handleVisibilityChange)
  }

  const release = async (): Promise<void> => {
    wantsLock = false
    const current = sentinel
    sentinel = null
    if (current) {
      detachSentinelListeners(current)
      try {
        await current.release()
      } catch {
        // Ignore release errors on expired sentinels.
      }
    }
    if (!destroyed && supported) {
      notify({
        status: 'idle',
        active: false,
        supported: true,
        errorMessage: null,
      })
    }
  }

  const destroy = (): void => {
    if (destroyed) return
    destroyed = true
    wantsLock = false
    if (doc) {
      doc.removeEventListener('visibilitychange', handleVisibilityChange)
    }
    const current = sentinel
    sentinel = null
    if (current) {
      detachSentinelListeners(current)
      void current.release().catch(() => {})
    }
    listeners.clear()
  }

  return {
    acquire,
    retry: acquire,
    release,
    destroy,
    getState: () => ({ ...state }),
    subscribe(listener: (next: WakeLockState) => void): () => void {
      listeners.add(listener)
      listener({ ...state })
      return () => {
        listeners.delete(listener)
      }
    },
  }
}
