'use strict'

/**
 * 极简 5 段 cron 支持（标准格式：分 时 日 月 周）。
 * 支持：星号、斜杠步进、a-b 区间、a,b,c 列表、单值。周 0 或 7 均表示周日。
 */

function parsePart(part, min, max) {
  const values = []
  for (let seg of part.split(',')) {
    seg = seg.trim()
    if (!seg) continue
    let step = 1
    let range = seg
    const slash = seg.split('/')
    if (slash.length > 1) {
      range = slash[0]
      step = parseInt(slash[1], 10) || 1
    }
    let a = min
    let b = max
    if (range !== '*') {
      const dash = range.split('-')
      if (dash.length === 2) {
        a = parseInt(dash[0], 10)
        b = parseInt(dash[1], 10)
      } else {
        a = b = parseInt(range, 10)
      }
    }
    if (Number.isNaN(a) || Number.isNaN(b)) throw new Error(`job-neo: bad cron field "${part}"`)
    for (let v = a; v <= b; v += step) if (v >= min && v <= max) values.push(v)
  }
  return [...new Set(values)].sort((x, y) => x - y)
}

const WILDCARD = /^\*(\/\d+)?$/

/**
 * 解析 cron 表达式，返回 { minutes, hours, doms, months, dows, domRestricted, dowRestricted }
 */
function parse(expr) {
  const parts = String(expr).trim().split(/\s+/)
  if (parts.length !== 5) throw new Error(`job-neo: cron must have 5 fields, got "${expr}"`)
  const minutes = parsePart(parts[0], 0, 59)
  const hours = parsePart(parts[1], 0, 23)
  const doms = parsePart(parts[2], 1, 31)
  const months = parsePart(parts[3], 1, 12)
  const dows = parsePart(parts[4], 0, 7).map(v => (v === 7 ? 0 : v))
  const domRestricted = !WILDCARD.test(parts[2].trim())
  const dowRestricted = !WILDCARD.test(parts[4].trim())
  return { minutes, hours, doms, months, dows, domRestricted, dowRestricted }
}

/**
 * 计算 from 之后（严格大于 from 所在分钟起点）的下一次运行时间。
 * @param {string} expr
 * @param {number|Date} from 默认 now
 * @returns {Date}
 */
function nextRun(expr, from = new Date()) {
  const c = typeof expr === 'string' ? parse(expr) : expr
  const { minutes, hours, doms, months, dows, domRestricted, dowRestricted } = c
  const minSet = new Set(minutes)
  const hourSet = new Set(hours)
  const domSet = new Set(doms)
  const monthSet = new Set(months)
  const dowSet = new Set(dows)

  const d = new Date(from.getTime())
  d.setSeconds(0, 0)
  d.setTime(d.getTime() + 60_000) // 从下一分钟起

  const domOk = (date) => {
    if (domRestricted && dowRestricted) {
      // 日和周都受限时：命中其一即可（标准 cron 行为）
      return domSet.has(date.getDate()) || dowSet.has(date.getDay())
    }
    if (domRestricted) return domSet.has(date.getDate())
    if (dowRestricted) return dowSet.has(date.getDay())
    return true
  }

  for (let i = 0; i < 366 * 30; i++) {
    if (!monthSet.has(d.getMonth() + 1)) {
      d.setMonth(d.getMonth() + 1, 1)
      d.setHours(0, 0, 0, 0)
      continue
    }
    if (!domOk(d)) {
      d.setDate(d.getDate() + 1)
      d.setHours(0, 0, 0, 0)
      continue
    }
    if (!hourSet.has(d.getHours())) {
      const next = hours.find(h => h > d.getHours())
      if (next === undefined) {
        d.setDate(d.getDate() + 1)
        d.setHours(0, 0, 0, 0)
        continue
      }
      d.setHours(next, 0, 0, 0)
      continue
    }
    if (!minSet.has(d.getMinutes())) {
      const next = minutes.find(m => m > d.getMinutes())
      if (next === undefined) {
        d.setHours(d.getHours() + 1, 0, 0, 0)
        continue
      }
      d.setMinutes(next, 0, 0, 0)
      continue
    }
    return d
  }
  throw new Error(`job-neo: no next run found for cron "${expr}"`)
}

module.exports = { parse, nextRun }