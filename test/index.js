'use strict'

// **Github:** https://github.com/teambition/timed-queue
//
// **License:** MIT
/* global describe, it, beforeEach */

var assert = require('assert')
var thunk = require('thunks')()
var TimedQueue = require('../index')

describe('timed-queue', function () {
  beforeEach(function (done) {
    var timedQueue = new TimedQueue({autoScan: false}).connect()
    timedQueue.queue('test')
    timedQueue.destroyQueue('test')(function () {
      this.close()
    })(done)
  })

  it('new TimedQueue()', function (done) {
    var timedQueue = new TimedQueue({interval: 2000})
    var events = []
    timedQueue
      .on('connect', function () {
        events.push('connect')
      })
      .on('scanStart', function () {
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
    timedQueue.connect()
  })

  it('timedQueue.scan, timedQueue.regulateFreq, timedQueue.close', function (done) {
    var timedQueue = new TimedQueue({interval: 1000})
    var scanCount = 0
    assert.strictEqual(timedQueue.delay, 1000)
    timedQueue.regulateFreq(-0.05)
    assert.strictEqual(timedQueue.delay, 1000)
    timedQueue.regulateFreq(-0.06)
    assert.strictEqual(timedQueue.delay < 1000, true)
    timedQueue.delay = 1000
    timedQueue.regulateFreq(0.05)
    assert.strictEqual(timedQueue.delay, 1000)
    timedQueue.regulateFreq(0.06)
    assert.strictEqual(timedQueue.delay > 1000, true)

    timedQueue
      .on('scanEnd', function () {
        if (++scanCount === 10) {
          timedQueue.close()
          done()
        }
        if (scanCount > 10) throw new Error('Should stopped!')
      })
    timedQueue.connect()
  })

  it('timedQueue.queue, queue.addjob, queue.show, queue.deljob, timedQueue.destroyQueue', function (done) {
    var timedQueue = new TimedQueue().connect()
    var queue = timedQueue.queue('test')

    queue.show(1)(function (err, res) {
      assert(err instanceof Error)
      return this.show('123')
    })(function (err, res) {
      assert.strictEqual(err, null)
      assert.strictEqual(res, null)
      return this.addjob('123', Date.now())
    })(function (err, res) {
      assert(err instanceof Error)
      return this.addjob('123', Date.now() + 1000)
    })(function (err, res) {
      assert.strictEqual(err, null)
      assert.strictEqual(res, 1)
      return this.show('123')
    })(function (err, res) {
      assert.strictEqual(err, null)
      assert.strictEqual(res.queue, 'test')
      assert.strictEqual(res.job, '123')
      assert.strictEqual(res.timing > Date.now(), true)
      assert.strictEqual(res.active, 0)
      return this.deljob('123')
    })(function (err, res) {
      assert.strictEqual(err, null)
      assert.strictEqual(res, 1)
      return this.show('123')
    })(function (err, res) {
      assert.strictEqual(err, null)
      assert.strictEqual(res, null)
      timedQueue.close()
    })(done)
  })

  it('queue.init, queue.getjobs, queue.showActive, queue.len, queue.ackjob', function (done) {
    var time = Date.now()
    var timedQueue = new TimedQueue({autoScan: false}).connect()
    var queue = timedQueue.queue('test', {
      count: 3,
      retry: 1000,
      expire: 3000,
      accuracy: 200
    })

    queue.getjobs()(function (err, res) {
      assert.strictEqual(err, null)
      assert.deepEqual(res, {
        retry: 1000,
        hasMore: 0,
        jobs: []
      })
      return this.addjob(
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
    })(function (err, res) {
      assert.strictEqual(err, null)
      assert.strictEqual(res, 10)
      return this.len()
    })(function (err, res) {
      assert.strictEqual(err, null)
      assert.strictEqual(res, 10)
      return thunk.delay.call(this, 1020)(function () {
        return this.getjobs()
      })
    })(function (err, res) {
      assert.strictEqual(err, null)
      assert.strictEqual(res.hasMore, 1)
      assert.deepEqual(res.jobs.map(function (job) {
        assert.strictEqual(job.retryCount, 0)
        assert.strictEqual(job.timing > time, true)
        assert.strictEqual(job.active > time, true)
        return job.job
      }), ['job0', 'job1', 'job2'])
      return this.showActive()
    })(function (err, res) {
      assert.strictEqual(err, null)
      assert.deepEqual(res.map(function (job) {
        assert.strictEqual(job.retryCount, 0)
        assert.strictEqual(job.timing > time, true)
        assert.strictEqual(job.active > time, true)
        return job.job
      }), ['job0', 'job1', 'job2'])
      return this.len()
    })(function (err, res) {
      assert.strictEqual(err, null)
      assert.strictEqual(res, 7)
      return this.getjobs()
    })(function (err, res) {
      assert.strictEqual(err, null)
      assert.strictEqual(res.hasMore, 0)
      assert.deepEqual(res.jobs.map(function (job) {
        assert.strictEqual(job.retryCount, 0)
        assert.strictEqual(job.timing > time, true)
        assert.strictEqual(job.active > time, true)
        return job.job
      }), ['job3', 'job4'])
      return thunk.delay.call(this, 1000)(function () {
        return this.getjobs()
      })
    })(function (err, res) {
      assert.strictEqual(err, null)
      assert.strictEqual(res.hasMore, 1)
      var retryJob = ['job0', 'job1', 'job2', 'job3', 'job4']
      assert.deepEqual(res.jobs.map(function (job) {
        assert.strictEqual(job.retryCount, retryJob.indexOf(job.job) >= 0 ? 1 : 0)
        assert.strictEqual(job.timing > time, true)
        assert.strictEqual(job.active > time, true)
        return job.job
      }), retryJob.concat('job5', 'job6', 'job7'))
      return this.getjobs()
    })(function (err, res) {
      assert.strictEqual(err, null)
      assert.strictEqual(res.hasMore, 0)
      assert.deepEqual(res.jobs.map(function (job) {
        assert.strictEqual(job.retryCount, 0)
        assert.strictEqual(job.timing > time, true)
        assert.strictEqual(job.active > time, true)
        return job.job
      }), ['job8'])
      return this.ackjob('job0', 'job1', 'job2', 'job3', 'job4', 'job5', 'job6', 'job7')
    })(function (err, res) {
      assert.strictEqual(err, null)
      assert.strictEqual(res, 8)
      return thunk.delay.call(this, 1000)(function () {
        return this.getjobs()
      })
    })(function (err, res) {
      assert.strictEqual(err, null)
      assert.strictEqual(res.hasMore, 0)
      assert.deepEqual(res.jobs.map(function (job) {
        assert.strictEqual(job.retryCount, job.job === 'job8' ? 1 : 0)
        assert.strictEqual(job.timing > time, true)
        assert.strictEqual(job.active > time, true)
        return job.job
      }), ['job8', 'job9'])
      timedQueue.close()
    })(done)
  })

  it('queue.scan', function (done) {
    var jobs = []
    var tasks = []
    var time = Date.now() + 100
    var timedQueue = new TimedQueue({autoScan: false}).connect()
    var queue = timedQueue.queue('test', {
      count: 8,
      retry: 1000,
      expire: 3000,
      accuracy: 300
    })

    for (var i = 0; i < 100; i++) tasks.push(i)

    queue.scan()(function (err, res) {
      assert.strictEqual(err instanceof Error, true)

      this.on('job', function (job) {
        jobs.push(job)
      })
      return this.scan()
    })(function (err, res) {
      assert.strictEqual(err, null)
      assert.deepEqual(res, [])
      assert.deepEqual(jobs, [])

      return thunk.all.call(this, tasks.map(function (index) {
        return queue.addjob(String(index), time + index * 100)
      }))(function (err, res) {
        if (err) throw err
        return this.len()
      })
    })(function (err, res) {
      assert.strictEqual(err, null)
      assert.strictEqual(res, 100)
      return this.scan()
    })(function (err, res) {
      assert.strictEqual(err, null)
      assert.strictEqual(res.length, 3)
      assert.strictEqual(Math.abs(res.reduce(function (m, v) { return m + v }, 0) / res.length) < 1, true)
      assert.strictEqual(jobs.length, 0) // jobs should be emit in next tick
      return thunk.delay.call(this, 1000)(function () {
        assert.deepEqual(jobs.map(function (job) {
          assert.strictEqual(job.timing >= time, true)
          return job.job
        }), ['0', '1', '2'])
        return this.scan()
      })
    })(function (err, res) {
      assert.strictEqual(err, null)
      assert.strictEqual(res.length, 10)
      assert.strictEqual(Math.abs(res.reduce(function (m, v) { return m + v }, 0) / res.length) < 1, true)
      return thunk.delay.call(this, 2000)(function () {
        assert.deepEqual(jobs.map(function (job) {
          assert.strictEqual(job.timing >= time, true)
          return job.job
        }), [
          '0', '1', '2', '0', '1', '2', '3', '4', '5', '6', '7',
          '8', '9', '10', '11', '12'
        ])
        jobs.length = 0
        return this.scan()
      })
    })(function (err, res) {
      assert.strictEqual(err, null)
      assert.strictEqual(res.length, 20)
      assert.strictEqual(Math.abs(res.reduce(function (m, v) { return m + v }, 0) / res.length) < 1, true)
      return thunk.delay.call(this)(function () {
        // '0', '1', '2' should expired
        var currentJobs = [
          '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15',
          '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27',
          '28', '29', '30', '31', '32'
        ]
        assert.deepEqual(jobs.map(function (job) {
          assert.strictEqual(job.timing >= time, true)
          return job.job
        }), currentJobs)
        return this.ackjob(currentJobs)
      })
    })(function (err, res) {
      assert.strictEqual(err, null)
      assert.strictEqual(res, 30)
      return this.scan()
    })(function (err, res) {
      assert.strictEqual(err, null)
      assert.deepEqual(res, [])
      timedQueue.close()
    })(done)
  })
})
