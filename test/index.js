'use strict'

// **Github:** https://github.com/teambition/timed-queue
//
// **License:** MIT
/* global describe, it */

var assert = require('assert')
var TimedQueue = require('../index')

describe('timed-queue', function () {
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
        timedQueue.stop()
        done()
      })
    timedQueue.connect()
  })

  it('timedQueue.scan, timedQueue.regulateFreq, timedQueue.stop', function (done) {
    var timedQueue = new TimedQueue({interval: 1000})
    var scanCount = 0
    assert.strictEqual(timedQueue.delay, 1000)
    timedQueue.regulateFreq(-0.05)
    assert.strictEqual(timedQueue.delay, 1000)
    timedQueue.regulateFreq(-0.2)
    assert.strictEqual(timedQueue.delay, 800)
    timedQueue.regulateFreq(-0.2)
    assert.strictEqual(timedQueue.delay, 640)
    timedQueue.regulateFreq(-0.9)
    assert.strictEqual(timedQueue.delay, 100)
    timedQueue.regulateFreq(1)
    assert.strictEqual(timedQueue.delay, 200)

    timedQueue
      .on('scanEnd', function () {
        if (++scanCount === 10) {
          timedQueue.stop()
          done()
        }
        if (scanCount > 10) throw new Error('Should stopped!')
      })
    timedQueue.connect()
  })
})
