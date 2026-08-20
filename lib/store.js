'use strict'

/**
 * 持久化层：基于 vkv-neo 的 KV 存储封装。
 * 表映射：
 *   job:<id>            → 单个任务的 JSON 串
 *   cron:<name>         → 定时任务定义的 JSON 串
 *   __ids               → 所有任务 id 的 JSON 数组
 *   __last:<name>       → 最近一次创建的 name 任务 id（用于按名称依赖）
 */
const { createKV } = require('vkv-neo')

const RAM = ':memory:'

class Store {
  /**
   * @param {object} opts { storage?/file?, mode?, sync? }
   */
  init(opts = {}) {
    const file = opts.storage || opts.file
    const isRam = file === RAM || file === ':ram:' || opts.memory === true
    this.kv = createKV({
      mode: opts.mode || 'auto',
      storage: isRam ? 'ram' : 'disk',
      file: isRam ? undefined : file,
      sync: opts.sync !== false
    })
    this._started = true
    return this
  }

  // ---------------------------------------------------------- job CRUD

  putJob(job) {
    this.kv.set(`job:${job.id}`, JSON.stringify(job))
    this._addId(job.id)
    this.kv.set(`__last:${job.name}`, job.id)
    return job
  }

  getJob(id) {
    const raw = this.kv.get(`job:${id}`)
    return raw == null ? null : JSON.parse(raw)
  }

  hasJob(id) {
    return this.kv.has(`job:${id}`)
  }

  deleteJob(id) {
    if (this.kv.del(`job:${id}`)) this._removeId(id)
  }

  /** 拉取全部任务记录（按创建顺序）。 */
  allJobs() {
    const ids = this._ids()
    if (ids.length === 0) return []
    const raw = this.kv.getMany(ids.map(id => `job:${id}`))
    const out = []
    for (let i = 0; i < ids.length; i++) {
      const v = raw[i]
      if (v != null) out.push(JSON.parse(v))
    }
    return out
  }

  // ---------------------------------------------------------- cron CRUD

  putCron(def) {
    this.kv.set(`cron:${def.name}`, JSON.stringify(def))
    return def
  }

  getCron(name) {
    const raw = this.kv.get(`cron:${name}`)
    return raw == null ? null : JSON.parse(raw)
  }

  allCrons() {
    const names = this._cronNames()
    if (names.length === 0) return []
    const raw = this.kv.getMany(names.map(n => `cron:${n}`))
    const out = []
    for (let i = 0; i < names.length; i++) {
      const v = raw[i]
      if (v != null) out.push(JSON.parse(v))
    }
    return out
  }

  deleteCron(name) {
    this.kv.del(`cron:${name}`)
    const names = this._cronNames().filter(n => n !== name)
    this.kv.set('__crons', JSON.stringify(names))
  }

  // ---------------------------------------------------------- index

  _addId(id) {
    const ids = this._ids()
    if (!ids.includes(id)) {
      ids.push(id)
      this.kv.set('__ids', JSON.stringify(ids))
    }
  }

  _removeId(id) {
    const ids = this._ids().filter(x => x !== id)
    this.kv.set('__ids', JSON.stringify(ids))
  }

  _ids() {
    const raw = this.kv.get('__ids')
    return raw == null ? [] : JSON.parse(raw)
  }

  _cronNames() {
    const raw = this.kv.get('__crons')
    return raw == null ? [] : JSON.parse(raw)
  }

  addCronName(name) {
    const names = this._cronNames()
    if (!names.includes(name)) {
      names.push(name)
      this.kv.set('__crons', JSON.stringify(names))
    }
  }

  lastJobOf(name) {
    return this.kv.get(`__last:${name}`)
  }

  flush() {
    if (this.kv) this.kv.flush()
  }

  close() {
    if (this.kv) this.kv.close()
    this._started = false
  }

  get size() {
    return this.kv ? this.kv.size : 0
  }

  get started() {
    return !!this._started
  }
}

module.exports = { Store, RAM }