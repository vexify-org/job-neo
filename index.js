'use strict'

/**
 * job-neo —— 持久化任务队列（vkv-neo 存储）。
 *
 *   const job = require('job-neo');
 *   await job.init({ storage: './data/queue.ndb' });
 *
 *   const id = await job.enqueue('send-email', { to: 'user@example.com' }, {
 *     delay: '5m', retry: 3, priority: 10
 *   });
 *
 *   await job.schedule('backup', {}, '0 2 * * *');
 *
 *   job.process('send-email', async (payload) => { ... });
 *
 *   await job.status(id); // pending / running / waiting / completed / failed / cancelled
 */
const { JobNeo } = require('./lib/queue')
const cron = require('./lib/cron')
const { parseDelay, UNITS } = require('./lib/time')

const singleton = new JobNeo()
singleton.cron = cron
singleton.parseDelay = parseDelay
singleton.timeUnits = UNITS

function createQueue(options) {
  const q = new JobNeo(options)
  q.cron = cron
  q.parseDelay = parseDelay
  q.timeUnits = UNITS
  return q
}

module.exports = singleton
module.exports.JobNeo = JobNeo
module.exports.createQueue = createQueue
module.exports.cron = cron
module.exports.parseDelay = parseDelay