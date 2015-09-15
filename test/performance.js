'use strict'

// **Github:** https://github.com/teambition/timed-queue
//
// **License:** MIT

var thunk = require('thunks')()
var TimedQueue = require('../index.js')
var timedQueue = new TimedQueue({autoScan: false})

timedQueue.connect()

var queue = timedQueue.queue('performance').on('job', function (job) {/* console.log(job) */})

thunk(function *() {
  console.log('jobs:', yield queue.len())
  console.log('add jobs:')
  var id = 0
  var batch = 10000

  var array = []
  while (array.length < 200) array.push(1)
  var time = Date.now()
  while (batch--) {
    yield array.map(function () {
      return queue.addjob(`performance_test:${id++}`, time + id * 1000)
    })
    if (!(batch % 100)) process.stdout.write('.')
  }

  console.log(`\n${id} jobs added, ${Date.now() - time} ms.`)
  console.log('current jobs:', yield queue.len())
  console.log('scan active jobs:')

  time = Date.now()
  var scan = yield queue.scan()
  console.log(`${scan.length} jobs scaned, ${Date.now() - time} ms.`)
  console.log('remain jobs:', yield queue.len())

  var activedJobs = yield queue.showActive()
  console.log('actived jobs:', activedJobs.length, 'a actived job:\n', activedJobs[activedJobs.length - 1])

  yield timedQueue.destroyQueue('performance')
  timedQueue.close()
})()
