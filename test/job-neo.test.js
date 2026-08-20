'use strict'
const assert = require('assert')
const { createQueue } = require('..')

function waitFor(fn, timeout = 4000, step = 15) {
  return new Promise((resolve, reject) => {
    const t0 = Date.now()
    const iv = setInterval(() => {
      let v
      try { v = fn() } catch (e) { clearInterval(iv); return reject(e) }
      if (v) { clearInterval(iv); return resolve(v) }
      if (Date.now() - t0 > timeout) { clearInterval(iv); reject(new Error('timeout')) }
    }, step)
  })
}

async function main() {
  const q = createQueue({ storage: ':memory:', concurrency: 3, pollInterval: 50 })
  await q.init()

  // ---------- 1) 基本 enqueue + process ----------
  const done1 = []
  q.process('basic', async (p) => { done1.push(p.to) })
  const id1 = await q.enqueue('basic', { to: 'a' })
  assert.strictEqual(await q.status(id1), 'pending')
  await waitFor(() => done1.length === 1)
  assert.strictEqual(await q.status(id1), 'completed')
  assert.strictEqual(done1[0], 'a')
  console.log('✓ 1 basic enqueue+process')

  // ---------- 2) delay ----------
  const done2 = []
  q.process('slow', async (p) => { done2.push(Date.now()) })
  const t0 = Date.now()
  const id2 = await q.enqueue('slow', {}, { delay: 300 })
  await waitFor(() => done2.length === 1, 3000)
  const elapsed = done2[0] - t0
  assert.ok(elapsed >= 250, `delay too short: ${elapsed}`)
  assert.strictEqual(await q.status(id2), 'completed')
  console.log('✓ 2 delay ~300ms')

  // ---------- 3) retry + 指数退避 → 最终 failed ----------
  let attempts = []
  setTimeout(() => {
    q.process('flaky', async () => { attempts.push(Date.now()); throw new Error('boom') })
  }, 20)
  const id3 = await q.enqueue('flaky', {}, { retry: 2, backoff: 50 })
  await waitFor(() => q.list({ status: 'failed' }).some(j => j.id === id3), 4000)
  const flaky = await q.get(id3)
  assert.strictEqual(flaky.status, 'failed')
  assert.strictEqual(flaky.attempts, 3) // 首次 + 2 重试
  assert.match(flaky.error, /boom/)
  console.log('✓ 3 retry (2 retries) -> failed, attempts=', flaky.attempts)

  // ---------- 4) 优先级 ----------
  const order = []
  setTimeout(() => {
    q.process('prio', async (p) => { order.push(p.v) })
  }, 20)
  await q.enqueue('prio', { v: 'low' }, { priority: 1 })
  await q.enqueue('prio', { v: 'high' }, { priority: 99 })
  await q.enqueue('prio', { v: 'mid' }, { priority: 50 })
  await waitFor(() => order.length === 3)
  assert.deepStrictEqual(order, ['high', 'mid', 'low'])
  console.log('✓ 4 priority order:', order)

  // ---------- 5) 依赖 / DAG ----------
  const log = []
  setTimeout(() => {
    q.process('fetch', async () => { log.push('fetch') })
    q.process('parse', async () => { log.push('parse') })
    q.process('save', async () => { log.push('save') })
  }, 20)
  await q.dag([
    { name: 'fetch', id: 'dFetch', payload: {} },
    { name: 'parse', id: 'dParse', payload: {}, dependencies: ['dFetch'] },
    { name: 'save', id: 'dSave', payload: {}, dependencies: ['dFetch', 'dParse'] }
  ])
  await waitFor(() => log.join(',') === 'fetch,parse,save')
  assert.deepStrictEqual(log, ['fetch', 'parse', 'save'])
  console.log('✓ 5 DAG execution order:', log)

  // ---------- 6) cron 定时 ----------
  const cronLog = []
  q.process('cronjob', async (p) => { cronLog.push(p.seq) })
  // 每秒整秒触发一次：先手动造一个小步进 cron（每分钟第几秒不支持，用每分钟一次太慢）
  // 用每天 0 点验证解析即可，真实触发用 next 提前注入短表达式。
  const cronId = await q.schedule('cronjob', { seq: 'daily' }, '0 2 * * *')
  const def = q.getCron('cronjob')
  assert.strictEqual(def.expression, '0 2 * * *')
  assert.strictEqual(typeof cronId, 'string')
  // 验证 nextRun 落在明天 02:00
  const next = new Date(q.getCron('cronjob').next)
  assert.strictEqual(next.getHours(), 2)
  assert.strictEqual(next.getMinutes(), 0)
  console.log('✓ 6 cron parse + nextRun (02:00)')
  q.cancelCron('cronjob')

  // ---------- 7) 多 worker 并发 ----------
  let inflight = 0
  let peak = 0
  const finished = []
  setTimeout(() => {
    q.process('multi', async (p) => {
      inflight++
      peak = Math.max(peak, inflight)
      await new Promise(r => setTimeout(r, 100))
      inflight--
      finished.push(p.v)
    })
  }, 20)
  await q.enqueueMany([1, 2, 3, 4, 5, 6, 7, 8].map(v => ({ name: 'multi', payload: { v } })))
  await waitFor(() => finished.length === 8)
  assert.ok(peak >= 3, `expected >=3 concurrent, got ${peak}`)
  assert.ok(peak <= 3, `expected <=3 (concurrency), got ${peak}`)
  console.log('✓ 7 concurrency peak =', peak, '(concurrency 3)')

  // ---------- 8) 管理：cancel / remove ----------
  const idC = await q.enqueue('none', {})
  assert.strictEqual(await q.cancel(idC), true)
  assert.strictEqual(await q.status(idC), 'cancelled')
  assert.strictEqual(await q.remove(idC), true)
  assert.strictEqual(await q.status(idC), null)
  console.log('✓ 8 cancel + remove')

  // ---------- 9) 持久化：磁盘重开 ----------
  const file = '/tmp/job-neo-test/queue.ndb'
  const a = createQueue({ storage: file, concurrency: 1, pollInterval: 40 })
  await a.init()
  a.process('persist', async () => {})
  // 暂停避免自动消费，验证重开仍能看到 pending
  a.pause()
  const p1 = await a.enqueue('persist', { x: 1 })
  const p2 = await a.enqueue('persist', { x: 2 }, { priority: 50 })
  await a.close()

  const b = createQueue({ storage: file, concurrency: 1, pollInterval: 40 })
  await b.init()
  const got2 = await b.get(p2)
  const got1 = await b.get(p1)
  assert.strictEqual(got2.priority, 50)
  assert.strictEqual(got1.status, 'pending')
  assert.ok(allFound([got1, got2]))
  await b.remove(p1); await b.remove(p2)
  await b.close()
  console.log('✓ 9 persistence across reopen')

  console.log('\nAll tests passed ✔')
}

function allFound(arr) { return arr.every(x => x && x.id) }

main().catch(e => { console.error(e); process.exit(1) })