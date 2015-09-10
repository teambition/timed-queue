'use strict'

// **Github:** https://github.com/teambition/timed-queue
//
// **License:** MIT

var TimedQueue = require('../index.js')
var timedQueue = new TimedQueue({interval: 1000 * 2})
var addLog = console.log.bind(console.log, 'addjob: ')
var ackLog = console.log.bind(console.log, 'ackjob: ')

// connect to redis cluster.
timedQueue
  .connect()
  .on('error', function (error) {
    console.error(error)
  })

// create 'event' job queue in timed-queue instance
var eventQueue = timedQueue.queue('event')

// add 'job' listener
eventQueue.on('job', function (job) {
  console.log('\n', job.job + ' at ' + new Date(job.timing) + ', actived: ' + new Date(job.active), '\n')
  this.ackjob(job.job)(ackLog)
  if (job.job === 'repeat event') this.addjob(job.job, job.timing + 10 * 1000)(addLog)
})

// add job to queue
eventQueue.addjob('repeat event', Date.now() + 5 * 1000)(addLog)
eventQueue.addjob('simple event', Date.now() + 10 * 1000)(addLog)
