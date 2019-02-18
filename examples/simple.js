'use strict'

// **Github:** https://github.com/teambition/timed-queue
//
// **License:** MIT

const TimedQueue = require('../index.js')
const timedQueue = new TimedQueue({ prefix: 'TQ', interval: 1000 * 2 })
const addLog = console.log.bind(console, 'addjob: ')
const ackLog = console.log.bind(console, 'ackjob: ')

// connect to redis.
timedQueue
  .connect()
  .on('error', function (error) {
    console.error(error)
  })

// create 'event' job queue in timed-queue instance
const eventQueue = timedQueue.queue('event')

// add 'job' listener
eventQueue.on('job', function (job) {
  console.log('\n', job.job + ' at ' + new Date(job.timing) + ', actived: ' + new Date(job.active), '\n')
  this.ackjob(job.job)(ackLog)
  if (job.job === 'repeat event') this.addjob(job.job, job.timing + 10 * 1000)(addLog)
})

// add job to queue
eventQueue.addjob('repeat event', Date.now() + 5 * 1000)(addLog)
eventQueue.addjob('simple event', Date.now() + 10 * 1000)(addLog)
