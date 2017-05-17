'use strict'

// **Github:** https://github.com/teambition/timed-queue
//
// **License:** MIT

const thunk = require('thunks')()
const TimedQueue = require('../index.js')
const timedQueue = new TimedQueue({autoScan: false})

timedQueue.connect()

const queue = timedQueue.queue('performance').on('job', function (job) { /* console.log(job) */ })

thunk(function * () {
  console.log('jobs:', yield queue.len())
  console.log('add jobs:')
  let id = 0
  let batch = 10000

  let array = []
  while (array.length < 200) array.push(1)
  let time = Date.now()
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
  let scan = yield queue.scan()
  console.log(`${scan.length} jobs scaned, ${Date.now() - time} ms.`)
  console.log('remain jobs:', yield queue.len())

  let activedJobs = yield queue.showActive()
  console.log('actived jobs:', activedJobs.length, 'a actived job:\n', activedJobs[activedJobs.length - 1])

  yield timedQueue.destroyQueue('performance')
  timedQueue.close()
})()

// jobs: 0
// add jobs:
// ....................................................................................................
// 2000000 jobs added, 68622 ms.
// current jobs: 2000000
// scan active jobs:
// 80 jobs scaned, 7 ms.
// remain jobs: 1999920
// actived jobs: 80 a actived job:
//  Job {
//   queue: 'performance',
//   job: 'performance_test:79',
//   timing: 1494938200330,
//   active: 1494938188959,
//   retryCount: 0 }
