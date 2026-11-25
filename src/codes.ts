// Unambiguous alphabet: omits 0/O and 1/I/L for easy reading across a room.
export const CODE_ALPHABET = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789'
export const ROOM_CODE_LENGTH = 4

export function generateRoomCode(length = ROOM_CODE_LENGTH): string {
  const bytes = new Uint8Array(length)
  if (typeof crypto !== 'undefined' && crypto.getRandomValues) {
    crypto.getRandomValues(bytes)
  } else {
    for (let i = 0; i < length; i += 1) bytes[i] = Math.floor(Math.random() * 256)
  }
  let code = ''
  for (let i = 0; i < length; i += 1) {
    code += CODE_ALPHABET[bytes[i] % CODE_ALPHABET.length]
  }
  return code
}

export function normalizeRoomCode(raw: string, length = ROOM_CODE_LENGTH): string {
  return raw
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '')
    .slice(0, length)
}

export function isValidRoomCode(code: string, length = ROOM_CODE_LENGTH): boolean {
  const normalized = normalizeRoomCode(code, length)
  if (normalized.length !== length) return false
  for (const ch of normalized) {
    if (!CODE_ALPHABET.includes(ch)) return false
  }
  return true
}

export function parseJoinCode(raw: string, length = ROOM_CODE_LENGTH): string {
  const queryMatch = /[?&](?:room|tv|pair)=([^&#\s]+)/i.exec(raw)
  if (queryMatch) {
    let extracted = queryMatch[1]
    try {
      extracted = decodeURIComponent(extracted)
    } catch {
      // Keep raw match when percent-encoding is malformed.
    }
    return normalizeRoomCode(extracted, length)
  }
  const pathMatch = /\/(?:tv|display)\/([A-Za-z0-9]{1,8})(?:[/?#]|$)/i.exec(raw)
  if (pathMatch) {
    return normalizeRoomCode(pathMatch[1], length)
  }
  return normalizeRoomCode(raw, length)
}

export function randomToken(length = 12): string {
  const alphabet = 'abcdefghijklmnopqrstuvwxyz0123456789'
  const bytes = new Uint8Array(length)
  if (typeof crypto !== 'undefined' && crypto.getRandomValues) {
    crypto.getRandomValues(bytes)
  } else {
    for (let i = 0; i < length; i += 1) bytes[i] = Math.floor(Math.random() * 256)
  }
  let out = ''
  for (let i = 0; i < length; i += 1) out += alphabet[bytes[i] % alphabet.length]
  return out
}

const DISPLAY_PATH_RE = /\/(?:tv|display)(?:\/[A-Za-z0-9]{1,8})?\/?$/i
const DISPLAY_CODE_PATH_RE = /\/(?:tv|display)\/([A-Za-z0-9]{1,8})\/?$/i

function rootPathname(pathname: string): string {
  const stripped = pathname.replace(DISPLAY_PATH_RE, '')
  return stripped || '/'
}

function displayPathname(pathname: string): string {
  const base = rootPathname(pathname)
  return base === '/' ? '/tv' : `${base.replace(/\/+$/, '')}/tv`
}

function clearNavigationParams(url: URL) {
  url.searchParams.delete('display')
  url.searchParams.delete('view')
  url.searchParams.delete('mode')
  url.searchParams.delete('tv')
  url.searchParams.delete('pair')
}

export function roomUrlFor(code: string, baseHref?: string, codeLength = ROOM_CODE_LENGTH): string {
  const clean = normalizeRoomCode(code, codeLength)
  const href = baseHref ?? (typeof window !== 'undefined' ? window.location.href : undefined)
  if (!href) return `?room=${clean}`
  const url = new URL(href)
  if (url.protocol !== 'file:') {
    url.pathname = rootPathname(url.pathname)
  }
  clearNavigationParams(url)
  url.searchParams.set('room', clean)
  url.hash = ''
  return url.toString()
}

export function displayUrlFor(
  code?: string | null,
  baseHref?: string,
  codeLength = ROOM_CODE_LENGTH,
): string {
  const clean = code ? normalizeRoomCode(code, codeLength) : ''
  const href = baseHref ?? (typeof window !== 'undefined' ? window.location.href : undefined)
  if (!href) return clean ? `/tv?room=${clean}` : '/tv'
  const url = new URL(href)
  clearNavigationParams(url)
  if (url.protocol === 'file:') {
    url.searchParams.set('display', '1')
  } else {
    url.pathname = displayPathname(url.pathname)
  }
  if (clean) url.searchParams.set('room', clean)
  else url.searchParams.delete('room')
  url.hash = ''
  return url.toString()
}

export function tvPairUrlFor(
  tvCode: string,
  baseHref?: string,
  codeLength = ROOM_CODE_LENGTH,
): string {
  const clean = normalizeRoomCode(tvCode, codeLength)
  const href = baseHref ?? (typeof window !== 'undefined' ? window.location.href : undefined)
  if (!href) return `?tv=${clean}`
  const url = new URL(href)
  if (url.protocol !== 'file:') {
    url.pathname = rootPathname(url.pathname)
  }
  clearNavigationParams(url)
  url.searchParams.delete('room')
  url.searchParams.set('tv', clean)
  url.hash = ''
  return url.toString()
}

export function readDisplayModeFromUrl(search?: string, pathname?: string): boolean {
  const rawSearch = search ?? (typeof window !== 'undefined' ? window.location.search : '')
  const rawPathname = pathname ?? (typeof window !== 'undefined' ? window.location.pathname : '')
  if (DISPLAY_PATH_RE.test(rawPathname)) return true
  const params = new URLSearchParams(rawSearch)
  const display = params.get('display')?.trim().toLowerCase() ?? ''
  if (['1', 'true', 'tv', 'yes'].includes(display)) return true
  const view = params.get('view')?.trim().toLowerCase() ?? ''
  if (['1', 'true', 'tv', 'display', 'game'].includes(view)) return true
  const mode = params.get('mode')?.trim().toLowerCase() ?? ''
  if (['tv', 'display'].includes(mode)) return true
  return false
}

export function readRoomFromUrl(
  search?: string,
  pathname?: string,
  codeLength = ROOM_CODE_LENGTH,
): string | null {
  const rawSearch = search ?? (typeof window !== 'undefined' ? window.location.search : '')
  const rawPathname = pathname ?? (typeof window !== 'undefined' ? window.location.pathname : '')
  const raw = new URLSearchParams(rawSearch).get('room')
  if (raw) {
    const clean = normalizeRoomCode(raw, codeLength)
    return clean.length === codeLength ? clean : null
  }
  const pathMatch = DISPLAY_CODE_PATH_RE.exec(rawPathname)
  if (!pathMatch) return null
  const clean = normalizeRoomCode(pathMatch[1], codeLength)
  return clean.length === codeLength ? clean : null
}

export function readTvPairFromUrl(search?: string, codeLength = ROOM_CODE_LENGTH): string | null {
  const rawSearch = search ?? (typeof window !== 'undefined' ? window.location.search : '')
  const params = new URLSearchParams(rawSearch)
  const raw = params.get('tv') ?? params.get('pair')
  if (!raw) return null
  const trimmed = raw.trim()
  if (trimmed === '1' || trimmed.toLowerCase() === 'true') return null
  const clean = normalizeRoomCode(trimmed, codeLength)
  return clean.length === codeLength ? clean : null
}

export function writeRoomToUrl(
  code: string | null,
  options?: { display?: boolean; codeLength?: number },
) {
  if (typeof window === 'undefined') return
  try {
    const url = new URL(window.location.href)
    const isFile = url.protocol === 'file:'
    clearNavigationParams(url)
    if (options?.display) {
      if (isFile) {
        url.searchParams.set('display', '1')
      } else {
        url.pathname = displayPathname(url.pathname)
      }
    } else if (!isFile) {
      url.pathname = rootPathname(url.pathname)
    }
    const clean = code ? normalizeRoomCode(code, options?.codeLength ?? ROOM_CODE_LENGTH) : ''
    if (clean) url.searchParams.set('room', clean)
    else url.searchParams.delete('room')
    window.history.replaceState(null, '', url.toString())
  } catch {
    // Ignore history restrictions in sandboxed/file:// contexts.
  }
}
