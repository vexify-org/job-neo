'use strict'

const crypto = require('crypto')
const { EventEmitter } = require('events')
const { Store, RAM } = require('./store')
const { parseDelay } = require('./time')
const cronUtil = require('./cron')

const ACTIVE = new Set(['pending', 'waiting'])
const TERMINAL = new Set(['completed', 'failed', 'cancelled'])
const RUNNING = 'running'

function uuid() {
  return crypto.randomUUID()
}

class JobNeo extends EventEmitter {
  constructor(options = {}) {
    super()
    this._options = options
    this._store = new Store()
    this._handlers = new Map()
    this._defs = new Map() // name -> { payload, opts } 可复用定义
    this._concurrency = options.concurrency || 4
    this._pollInterval = options.pollInterval || 200
    this._started = false
    this._paused = false
    this._timer = null
    this._active = 0
    this._runQueue = [] // 已认领、待执行的任务（按优先级排序）
    this._initialized = false
    this._initPromise = null
  }

  // ============================================================
  // 初始化 / 生命周期
  // ============================================================

  /**
   * @param {object} opts { storage?: 路径 | ':memory:', mode?, concurrency?, pollInterval? }
   */
  async init(opts = {}) {
    if (this._initialized) return this
    const merged = { ...this._options, ...opts }
    this._store.init({
      storage: merged.storage || RAM,
      file: merged.file || merged.storage,
      mode: merged.mode,
      sync: merged.sync
    })
    if (merged.concurrency != null) this._concurrency = merged.concurrency
    if (merged.pollInterval != null) this._pollInterval = merged.pollInterval
    this._recoverRunning()
    this._initialized = true
    this.start()
    return this
  }

  _ensureInit() {
    if (this._initialized) return
    // 懒初始化：默认持久化到 ./job-neo.ndb
    this._store.init({
      storage: process.env.JOB_NEO_STORAGE || require('path').join(process.cwd(), 'job-neo.ndb'),
      sync: true
    })
    this._recoverRunning()
    this._initialized = true
    this.start()
  }

  /** 进程崩溃恢复：把遗留的 running 任务重置回 pending。 */
  _recoverRunning() {
    for (const job of this._store.allJobs()) {
      if (job.status === RUNNING) {
        job.status = 'pending'
        this._store.putJob(job)
      }
    }
  }

  start() {
    this._ensureInit()
    if (this._started && this._timer) return this
    this._started = true
    this._paused = false
    this._timer = setInterval(() => {
      try {
        this._tick()
      } catch (e) {
        this.emit('error', e)
      }
    }, this._pollInterval)
    if (this._timer.unref) this._timer.unref()
    // 启动即侦测一次（立即分发已到期任务）
    setImmediate(() => this._tick())
    return this
  }

  stop() {
    if (this._timer) {
      clearInterval(this._timer)
      this._timer = null
    }
    this._started = false
    return this
  }

  pause() {
    this._paused = true
    return this
  }

  resume() {
    this._paused = false
    this._kick()
    return this
  }

  setConcurrency(n) {
    this._concurrency = Math.max(1, Math.floor(n) || 1)
    this._kick()
    return this
  }

  setPollInterval(ms) {
    this._pollInterval = Math.max(20, ms)
    if (this._started) {
      this.stop()
      this.start()
    }
    return this
  }

  async close() {
    this.stop()
    await this._idle()
    this._store.flush()
    this._store.close()
    this._initialized = false
  }

  _idle() {
    if (this._active === 0) return Promise.resolve()
    return new Promise(resolve => this.once('drained', resolve))
  }

  _kick() {
    if (this._initialized && this._started) setImmediate(() => this._tick())
  }

  // ============================================================
  // 创建任务
  // ============================================================

  /**
   * 入队一个任务。
   * opts: { delay, retry|maxRetries, priority, dependencies|dependsOn, backoff, tags, id, id, scheduleId, schedule }
   * @returns {Promise<string>} job id
   */
  async enqueue(name, payload, opts = {}) {
    this._ensureInit()
    const id = opts.id || uuid()
    const maxRetries = opts.retry != null ? opts.retry : opts.maxRetries != null ? opts.maxRetries : 0
    const delayMs = parseDelay(opts.delay)
    const deps = opts.dependencies || opts.dependsOn || null

    const job = {
      id,
      name,
      payload: payload === undefined ? null : payload,
      status: 'pending',
      priority: opts.priority || 0,
      retry: Math.max(0, Math.floor(maxRetries) || 0),
      attempts: 0,
      runAt: Date.now() + delayMs,
      delay: delayMs,
      dependencies: deps ? (Array.isArray(deps) ? deps : [deps]) : null,
      backoff: this._resolveBackoff(opts.backoff),
      schedule: opts.schedule || null,
      scheduleId: opts.scheduleId || null,
      tags: opts.tags || null,
      createdAt: Date.now(),
      startedAt: null,
      completedAt: null,
      error: null
    }
    this._store.putJob(job)
    this.emit('enqueued', job)
    this._kick()
    return id
  }

  /**
   * 批量入队。items: [{ name, payload, options }]，返回 rid
   */
  async enqueueMany(items) {
    const ids = []
    for (const it of items) {
      ids.push(await this.enqueue(it.name, it.payload, it.options))
    }
    return ids
  }

  /**
   * 注册一个可复用定义，供 create 出来的 DAG 按名称依赖。
   */
  create(name, definition = {}) {
    this._defs.set(name, definition)
    return name
  }

  /**
   * 以图谱形式一次性入队 DAG。
   * nodes: [{ name, id?, payload, dependencies?: [name|id], ...options }]
   * @returns {Promise<Record<string,string>>} name -> id
   */
  async dag(nodes) {
    this._ensureInit()
    const idMap = {}
    for (const node of nodes) {
      const id = node.id || uuid()
      idMap[node.id || node.name] = id
      node.__id = id
    }
    const result = {}
    for (const node of nodes) {
      const deps = (node.dependencies || []).map(ref => idMap[ref] || ref)
      const opts = { ...node.options }
      if (deps.length) opts.dependencies = deps
      if (node.id !== undefined) opts.id = node.__id
      const id = await this.enqueue(node.name, node.payload, opts)
      result[node.id || node.name] = id
    }
    return result
  }

  /**
   * 创建周期任务。
   * @returns {Promise<string>} cron id
   */
  async schedule(name, payload, cronExpr, opts = {}) {
    this._ensureInit()
    const expr = String(cronExpr).trim()
    cronUtil.parse(expr) // 校验
    const existing = this._store.getCron(name)
    const id = existing ? existing.id : uuid()
    const def = {
      id,
      name,
      payload: payload === undefined ? null : payload,
      expression: expr,
      options: opts || {},
      next: existing ? existing.next : cronUtil.nextRun(expr).getTime(),
      lastRunAt: existing ? existing.lastRunAt : null,
      overlap: !!opts.overlap,
      enabled: opts.enabled !== false,
      createdAt: (existing && existing.createdAt) || Date.now()
    }
    this._store.putCron(def)
    this._store.addCronName(name)
    this.emit('scheduled', def)
    this._kick()
    return id
  }

  cancelCron(name) {
    this._store.deleteCron(name)
    return this
  }

  removeCron(name) {
    return this.cancelCron(name)
  }

  getCron(name) {
    return this._store.getCron(name)
  }

  crons() {
    return this._store.allCrons()
  }

  // ============================================================
  // 消费
  // ============================================================

  process(name, handler) {
    if (typeof handler !== 'function') throw new Error('job-neo: handler must be a function')
    this._handlers.set(name, handler)
    this._kick()
    return this
  }

  processMany(handlers) {
    for (const [name, h] of Object.entries(handlers)) this.process(name, h)
    return this
  }

  unprocess(name) {
    this._handlers.delete(name)
    return this
  }

  // ============================================================
  // 查询 / 管理
  // ============================================================

  async status(id) {
    this._ensureInit()
    const job = this._store.getJob(id)
    return job ? job.status : null
  }

  async get(id) {
    this._ensureInit()
    return this._store.getJob(id)
  }

  list(filter = {}) {
    this._ensureInit()
    let jobs = this._store.allJobs()
    if (filter.status) jobs = jobs.filter(j => j.status === filter.status)
    if (filter.name) jobs = jobs.filter(j => j.name === filter.name)
    return jobs
  }

  async cancel(id) {
    this._ensureInit()
    const job = this._store.getJob(id)
    if (!job) return false
    if (job.status === RUNNING || TERMINAL.has(job.status)) return false
    job.status = 'cancelled'
    job.completedAt = Date.now()
    this._store.putJob(job)
    this.emit('cancelled', job)
    return true
  }

  /** 强制重跑一个 failed/cancelled/completed 任务。 */
  async retry(id, opts = {}) {
    this._ensureInit()
    const job = this._store.getJob(id)
    if (!job) return false
    job.status = 'pending'
    job.attempts = opts.resetAttempts ? 0 : job.attempts
    job.error = null
    job.startedAt = null
    job.completedAt = null
    job.runAt = Date.now() + parseDelay(opts.delay)
    this._store.putJob(job)
    this._kick()
    return true
  }

  async remove(id) {
    this._ensureInit()
    const job = this._store.getJob(id)
    if (!job) return false
    if (job.status === RUNNING) return false
    this._store.deleteJob(id)
    this.emit('removed', job)
    return true
  }

  // ============================================================
  // 调度核心
  // ============================================================

  _tick() {
    if (!this._initialized || this._paused || this._started === false) {
      if (this._paused) return
      if (!this._started && !this._paused) return
      return
    }
    const now = Date.now()

    // 1) 触发到期 cron
    this._fireCrons(now)

    // 2) 认领到期任务
    const ready = this._collectReady(now)
    const free = this._concurrency - this._active
    const toClaim = ready.slice(0, Math.max(0, free))

    if (toClaim.length === 0) return
    this._active += toClaim.length
    for (const job of toClaim) {
      job.status = RUNNING
      job.startedAt = now
      job.attempts = (job.attempts || 0) + 1
      this._store.putJob(job)
      this.emit('started', job)
      this._run(job)
    }
  }

  /** 收集所有到期且可执行（依赖满足、有处理器）的任务，按优先级排序。 */
  _collectReady(now) {
    const handlers = this._handlers
    const jobs = this._store.allJobs()
    const byId = new Map(jobs.map(j => [j.id, j]))

    const ready = []
    for (const job of jobs) {
      if (!ACTIVE.has(job.status)) continue
      if (job.runAt > now) continue
      if (!handlers.has(job.name)) continue

      if (job.dependencies && job.dependencies.length) {
        const dep = this._evalDeps(job.dependencies, byId, handlers)
        if (dep.failed) {
          job.status = 'failed'
          job.error = dep.error
          job.completedAt = now
          this._store.putJob(job)
          this.emit('failed', job)
          continue
        }
        if (!dep.satisfied) {
          if (job.status !== 'waiting') {
            job.status = 'waiting'
            this._store.putJob(job)
          }
          continue
        }
        if (job.status === 'waiting') {
          job.status = 'pending'
          this._store.putJob(job)
        }
      }

      ready.push(job)
    }

    ready.sort((a, b) => {
      if (a.priority !== b.priority) return b.priority - a.priority // 高优先级在前
      if (a.runAt !== b.runAt) return a.runAt - b.runAt
      return a.createdAt - b.createdAt
    })
    return ready
  }

  /** 评估某个任务的依赖是否满足。返回 { satisfied, failed, error } */
  _evalDeps(deps, byId, _handlers) {
    const nameToId = new Map()
    for (const job of byId.values()) nameToId.set(job.name, job.id)
    for (const ref of deps) {
      const depJob = byId.get(ref) || byId.get(nameToId.get(ref))
      if (!depJob) return { satisfied: false, failed: false } // 依赖尚未创建，继续等待
      if (TERMINAL.has(depJob.status)) {
        if (depJob.status !== 'completed') {
          return { satisfied: false, failed: true, error: `dependency failed: ${depJob.name} (${depJob.status})` }
        }
        // completed → 满足
        continue
      }
      return { satisfied: false, failed: false }
    }
    return { satisfied: true, failed: false }
  }

  _fireCrons(now) {
    for (const cron of this._store.allCrons()) {
      if (!cron.enabled) continue
      if (cron.next > now) continue
      // 防重叠：若同一 cron 还有在跑/待跑实例则跳过（除非 overlap）
      if (!cron.overlap && this._cronActive(cron.name)) continue
      this.enqueue(cron.name, cron.payload, { scheduleId: cron.name, schedule: cron.expression })
      cron.lastRunAt = now
      cron.next = cronUtil.nextRun(cron.expression).getTime()
      this._store.putCron(cron)
    }
  }

  _cronActive(name) {
    for (const job of this._store.allJobs()) {
      if (job.scheduleId === name && ACTIVE.has(job.status)) return true
    }
    return false
  }

  // ============================================================
  // 执行
  // ============================================================

  async _run(job) {
    try {
      const handler = this._handlers.get(job.name)
      const ctx = {
        id: job.id,
        name: job.name,
        attempt: job.attempts,
        payload: job.payload,
        queue: this,
        retry: (delay) => this._scheduleRetry(job, delay)
      }
      await handler(job.payload, ctx)
      this._succeed(job)
    } catch (err) {
      this._fail(job, err)
    }
  }

  _succeed(job) {
    const fresh = this._store.getJob(job.id)
    const target = fresh || job
    target.status = 'completed'
    target.completedAt = Date.now()
    target.error = null
    this._store.putJob(target)
    this.emit('completed', target)
    this._settled()
  }

  _fail(job, err) {
    const message = err instanceof Error ? err.message : String(err)
    const fresh = this._store.getJob(job.id)
    const target = fresh || job

    if (target.attempts <= target.retry) {
      // 指数/固定退避后重试
      const delay = this._backoffDelay(target)
      target.status = 'pending'
      target.error = message
      target.startedAt = null
      target.runAt = Date.now() + delay
      this._store.putJob(target)
      this.emit('retrying', { ...target, nextAttemptAt: target.runAt })
      this._settled()
      this._kick()
      return
    }

    target.status = 'failed'
    target.error = message
    target.completedAt = Date.now()
    this._store.putJob(target)
    this.emit('failed', target)
    this._settled()
  }

  /** 处理器内手动重试。 */
  _scheduleRetry(job, delayMs) {
    const fresh = this._store.getJob(job.id)
    const target = fresh || job
    target.status = 'pending'
    target.error = target.error || 'retry requested'
    target.startedAt = null
    target.runAt = Date.now() + parseDelay(delayMs)
    this._store.putJob(target)
    this.emit('retrying', { ...target, nextAttemptAt: target.runAt })
    this._settled()
    this._kick()
  }

  _settled() {
    this._active = Math.max(0, this._active - 1)
    if (this._active === 0) this.emit('drained')
    this._kick()
  }

  // ============================================================
  // 工具
  // ============================================================

  _resolveBackoff(b) {
    if (b == null) return { type: 'exponential', base: 1000, factor: 2, max: 300_000 }
    if (typeof b === 'number' || typeof b === 'string') {
      const base = parseDelay(b)
      return { type: 'exponential', base: base || 1000, factor: 2, max: 300_000 }
    }
    if (typeof b === 'object') {
      const base = b.base != null ? b.base : b.interval != null ? parseDelay(b.interval) : 1000
      return {
        type: b.type || 'exponential',
        base,
        factor: b.factor || 2,
        max: b.max != null ? b.max : 300_000
      }
    }
    return { type: 'exponential', base: 1000, factor: 2, max: 300_000 }
  }

  _backoffDelay(job) {
    const b = job.backoff || { type: 'exponential', base: 1000, factor: 2, max: 300_000 }
    const attempts = Math.max(1, job.attempts)
    if (b.type === 'fixed') return Math.min(b.base, b.max || Number.MAX_SAFE_INTEGER)
    const delay = b.base * Math.pow(b.factor || 2, attempts - 1)
    return Math.round(Math.min(delay, b.max || Number.MAX_SAFE_INTEGER))
  }

  get concurrency() {
    return this._concurrency
  }

  get active() {
    return this._active
  }

  get size() {
    return this._store.size
  }
}

module.exports = { JobNeo, uuid }