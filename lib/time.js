'use strict'

/**
 * 延迟时间解析：将 '5m' / '10s' / '2h' / '1d' 或数字(毫秒) 解析为毫秒。
 */
const UNITS = { ms: 1, s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000, w: 604_800_000 }
const RE = /^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d|w)?$/

function parseDelay(input) {
  if (input == null || input === 0) return 0
  if (input instanceof Date) return Math.max(0, input.getTime() - Date.now())
  if (typeof input === 'number') return input
  if (typeof input === 'string') {
    const s = input.trim().toLowerCase()
    if (RE.test(s)) {
      const m = s.match(RE)
      const n = parseFloat(m[1])
      const u = m[2] || 'ms'
      if (Number.isFinite(n)) return Math.max(0, Math.round(n * UNITS[u]))
    }
    const num = Number(s)
    if (!Number.isNaN(num)) return num
    throw new Error(`job-neo: cannot parse delay "${input}"`)
  }
  throw new Error(`job-neo: invalid delay "${input}"`)
}

module.exports = { parseDelay, UNITS }