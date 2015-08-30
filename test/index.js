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
      .on('scanEnd', function (queueLength, time) {
        events.push('scanEnd')
        assert.strictEqual(queueLength, 0)
        assert.strictEqual(time >= 0, true)
        assert.deepEqual(events, ['connect', 'scanStart', 'scanEnd'])
        done()
      })
    timedQueue.connect()
  })
})
