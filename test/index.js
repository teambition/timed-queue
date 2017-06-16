'use strict'

// **Github:** https://github.com/teambition/timed-queue
//
// **License:** MIT

const tman = require('tman')
const assert = require('assert')
const thunk = require('thunks')()
const redis = require('thunk-redis')
const thunkQueue = require('thunk-queue')
const TimedQueue = require('../index')

tman.suite('timed-queue', function () {
  this.timeout(50000)

  const queueNames = []
  function getName () {
    let name = `test_${Date.now()}_${queueNames.length}`
    queueNames.push(name)
    return name
  }

  tman.afterEach(function * () {
    let timedQueue = new TimedQueue({autoScan: false}).connect()
    // ensure to clean the test queue
    for (let name of queueNames) {
      yield timedQueue.destroyQueue(name)
    }
    queueNames.length = 0
    timedQueue.close()
  })

  tman.it('new TimedQueue()', function (done) {
    const timedQueue = new TimedQueue({interval: 2000})
    const events = []
    timedQueue
      .on('connect', function () {
        events.push('connect')
      })
      .on('scanStart', function (n) {
        events.push('scanStart')
      })
      .on('scanEnd', function (queues, time) {
        events.push('scanEnd')
        assert.strictEqual(queues, 0)
        assert.strictEqual(time >= 0, true)
        assert.deepEqual(events, ['connect', 'scanStart', 'scanEnd'])
        timedQueue.close()
        done()
      })
    timedQueue.connect(redis.createClient())
  })

  tman.it('timedQueue.scan, timedQueue.regulateFreq, timedQueue.close', function (done) {
    const timedQueue = new TimedQueue({interval: 1000})
    let scanCount = 0
    assert.strictEqual(timedQueue.delay, 1000)
    timedQueue.regulateFreq(-0.05)
    assert.strictEqual(timedQueue.delay, 1000)
    timedQueue.regulateFreq(-0.06)
    assert.strictEqual(timedQueue.delay < 1000, true)
    timedQueue.delay = 1000
    timedQueue.regulateFreq(0.05)
    assert.strictEqual(timedQueue.delay, 1000)
    timedQueue.regulateFreq(0.06)
    assert.strictEqual(timedQueue.delay, 1000)

    timedQueue
      .on('scanEnd', function () {
        if (++scanCount === 10) {
          timedQueue.close()
          done()
        }
        if (scanCount > 10) throw new Error('Should stopped!')
      })
    timedQueue.connect(redis.createClient())
  })

  tman.it('timedQueue.queue, queue.addjob, queue.show, queue.deljob, timedQueue.destroyQueue', function * () {
    const timedQueue = new TimedQueue().connect()
    const name = getName()
    const queue = timedQueue.queue(name)

    yield queue.show(1)((err) => assert(err instanceof Error))

    let res = yield queue.show('123')
    assert.strictEqual(res, null)

    yield queue.addjob('123', Date.now())((err) => assert(err instanceof Error))
    res = yield queue.addjob('123', Date.now() + 1000)
    assert.strictEqual(res, 1)

    res = yield queue.show('123')
    assert.strictEqual(res.queue, name)
    assert.strictEqual(res.job, '123')
    assert.strictEqual(res.timing > Date.now(), true)
    assert.strictEqual(res.active, 0)

    res = yield queue.deljob('123')
    assert.strictEqual(res, 1)

    res = yield queue.show('123')
    assert.strictEqual(res, null)
    timedQueue.close()
  })

  tman.it('queue.init, queue.getjobs, queue.showActive, queue.len, queue.ackjob', function * () {
    let time = Date.now()
    const timedQueue = new TimedQueue({autoScan: false}).connect(redis.createClient())
    const queue = timedQueue.queue(getName(), {
      count: 3,
      retry: 1000,
      expire: 3000,
      accuracy: 200
    })

    let res = yield queue.getjobs()
    assert.deepEqual(res, {
      retry: 1000,
      hasMore: 0,
      jobs: []
    })

    res = yield queue.addjob(
      'job0', time + 1000,
      'job1', time + 1010,
      'job2', time + 1020,
      'job3', time + 1030,
      'job4', time + 1100,
      'job5', time + 1300,
      'job6', time + 1320,
      'job7', time + 1340,
      'job8', time + 1360,
      'job9', time + 3000
    )
    assert.strictEqual(res, 10)

    res = yield queue.len()
    assert.strictEqual(res, 10)

    yield thunk.delay(1020)

    res = yield queue.getjobs()
    assert.strictEqual(res.hasMore, 1)
    assert.deepEqual(res.jobs.map((job) => {
      assert.strictEqual(job.retryCount, 0)
      assert.strictEqual(job.timing > time, true)
      assert.strictEqual(job.active > time, true)
      return job.job
    }), ['job0', 'job1', 'job2'])

    res = yield queue.showActive()
    assert.deepEqual(res.map((job) => {
      assert.strictEqual(job.retryCount, 0)
      assert.strictEqual(job.timing > time, true)
      assert.strictEqual(job.active > time, true)
      return job.job
    }), ['job0', 'job1', 'job2'])

    res = yield queue.len()
    assert.strictEqual(res, 7)

    res = yield queue.getjobs()
    assert.strictEqual(res.hasMore, 0)
    assert.deepEqual(res.jobs.map((job) => {
      assert.strictEqual(job.retryCount, 0)
      assert.strictEqual(job.timing > time, true)
      assert.strictEqual(job.active > time, true)
      return job.job
    }), ['job3', 'job4'])

    yield thunk.delay(1000)
    res = yield queue.getjobs()
    assert.strictEqual(res.hasMore, 1)
    let retryJob = ['job0', 'job1', 'job2', 'job3', 'job4']
    assert.deepEqual(res.jobs.map((job) => {
      assert.strictEqual(job.retryCount, retryJob.indexOf(job.job) >= 0 ? 1 : 0)
      assert.strictEqual(job.timing > time, true)
      assert.strictEqual(job.active > time, true)
      return job.job
    }), retryJob.concat('job5', 'job6', 'job7'))

    res = yield queue.getjobs()
    assert.strictEqual(res.hasMore, 0)
    assert.deepEqual(res.jobs.map((job) => {
      assert.strictEqual(job.retryCount, 0)
      assert.strictEqual(job.timing > time, true)
      assert.strictEqual(job.active > time, true)
      return job.job
    }), ['job8'])

    res = yield queue.ackjob('job0', 'job1', 'job2', 'job3', 'job4', 'job5', 'job6', 'job7')
    assert.strictEqual(res, 8)

    yield thunk.delay(1000)
    res = yield queue.getjobs()
    assert.strictEqual(res.hasMore, 0)
    assert.deepEqual(res.jobs.map((job) => {
      assert.strictEqual(job.retryCount, job.job === 'job8' ? 1 : 0)
      assert.strictEqual(job.timing > time, true)
      assert.strictEqual(job.active > time, true)
      return job.job
    }), ['job8', 'job9'])
    timedQueue.close()
  })

  tman.it('queue.scan', function * () {
    const jobs = []
    const tasks = []

    let time = Date.now() + 100
    const timedQueue = new TimedQueue({autoScan: false}).connect(redis.createClient())
    const queue = timedQueue.queue(getName(), {
      count: 8,
      retry: 1000,
      expire: 3000,
      accuracy: 300
    })

    for (let i = 0; i < 100; i++) tasks.push(i)

    yield queue.scan()((err) => assert.ok(err instanceof Error))

    queue.on('job', function (job) {
      jobs.push(job)
    })
    yield queue.scan()
    assert.deepEqual(jobs, [])

    yield tasks.map((index) => queue.addjob(String(index), time + index * 100))
    let res = yield queue.len()
    assert.strictEqual(res, 100)

    res = yield queue.scan()
    assert.strictEqual(res.length, 3)
    assert.strictEqual(Math.abs(res.reduce((m, v) => { return m + v }, 0) / res.length) < 1, true)
    assert.strictEqual(jobs.length, 0) // jobs should be emit in next tick

    yield thunk.delay(1000)
    assert.deepEqual(jobs.map((job) => {
      assert.strictEqual(job.timing >= time, true)
      return job.job
    }), ['0', '1', '2'])

    res = yield queue.scan()
    assert.strictEqual(res.length, 10)
    assert.strictEqual(Math.abs(res.reduce((m, v) => { return m + v }, 0) / res.length) < 1, true)

    yield thunk.delay(2000)
    assert.deepEqual(jobs.map((job) => {
      assert.strictEqual(job.timing >= time, true)
      return job.job
    }), [
      '0', '1', '2', '0', '1', '2', '3', '4', '5', '6', '7',
      '8', '9', '10', '11', '12'
    ])
    jobs.length = 0

    res = yield queue.scan()
    assert.strictEqual(res.length, 20)
    assert.strictEqual(Math.abs(res.reduce((m, v) => { return m + v }, 0) / res.length) < 1, true)
    yield thunk.delay()
    // '0', '1', '2' should expired
    let currentJobs = [
      '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15',
      '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27',
      '28', '29', '30', '31', '32'
    ]
    assert.deepEqual(jobs.map((job) => {
      assert.strictEqual(job.timing >= time, true)
      return job.job
    }), currentJobs)

    res = yield queue.ackjob(currentJobs)
    assert.strictEqual(res, 30)

    res = yield queue.scan()
    assert.deepEqual(res, [])
    timedQueue.close()
  })

  tman.suite('chaos: 100000 random jobs', function () {
    const jobs = []
    const check = new Map()

    tman.it('ok', function (done) {
      const queueT = thunkQueue()
      const timedQueue = new TimedQueue({interval: 1000}).connect(redis.createClient())
      const queues = [timedQueue.queue(getName()), timedQueue.queue(getName()), timedQueue.queue(getName())]

      let i = 100000
      while (i--) jobs.push(String(i))

      for (let queue of queues) {
        queue.on('job', (job) => {
          if (check.get(job.job) !== job.timing) {
            queueT.push(() => { throw new Error(`uncaughtException: ${check[job.job]}, ${JSON.stringify(job)}`) })
          } else {
            check.delete(job.job)
            queueT.push(queue.ackjob(job.job)((err) => {
              if (err) throw err
            }))
          }
          if (!check.size) queueT.end()
        })
      }

      thunk(function * () {
        while (jobs.length) {
          let i = 0
          let list = []
          let time = Date.now() + 2000
          let seed = Math.ceil(Math.random() * 10000)
          // push random jobs
          while (jobs.length && i++ < seed) {
            let job = jobs.pop()
            check.set(job, time + (i < 3000 ? i : Math.floor(i / 3)))
            list.push(queues[i % 3].addjob(job, check.get(job)))
          }

          yield list
          yield thunk.delay(seed / 10)
        }

        // wait for jobs ACK
        yield queueT
      })(done)
    })
  })
})
