// **Github:** https://github.com/teambition/timed-queue
//
// **License:** MIT

'use strict'

var util = require('util')
var thunk = require('thunks')()
var redis = require('thunk-redis')
var EventEmitter = require('events').EventEmitter

module.exports = TimedQueues

function TimedQueues (options) {
  this.options = options || {}
  this.prefix = options.prefix || 'TIMED'
  this.interval = options.interval || 1000 * 60
  this.ackTimeout = options.ackTimeout || 1000 * 60
  this.expire = options.expire || 1000 * 60 * 5
  this.count = options.count || 20
  this.queues = Object.create(null)
  this.redis = null
}

TimedQueues.prototype.connect = function () {
  this.redis = redis.createClient.apply(null, arguments)
  return this
}

TimedQueues.prototype.scan = function (queueName, count) {}

TimedQueues.prototype.queue = function (queueName, options) {
  validateString(queueName)
  if (!this.queues[queueName]) this.queues[queueName] = new Queue(this, queueName)
  if (options) this.queues[queueName].updateOptions(options)
  return this.queues[queueName]
}

function Queue (timedQueues, queueName) {
  this.root = timedQueues
  this.name = queueName
  this.queueKey = '{' + timedQueues.prefix + ':' + queueName + '}'
  this.activeQueueKey = '{' + timedQueues.prefix + ':' + queueName + '}='
  EventEmitter.call(this)
}

util.inherits(Queue, EventEmitter)

Queue.prototype.updateOptions = function (options) {}

Queue.prototype.addjob = function (jobId, time) {
  var args = slice(arguments)

  return thunk.call(this, function (done) {
    var time = 0
    var current = Date.now()

    for (var i = 0, l = args.length || 2; i < l; i += 2) {
      validateString(args[i])
      time = Math.floor(args[i + 1])
      if (!time || time <= current) throw new Error(String(time) + ' is invalid time')
    }

    args.unshift(this.queueKey)
    this.root.redis.zadd(args)(done)
  })
}

Queue.prototype.getjob = function (jobId) {
  return thunk.call(this, function (done) {
    var ctx = this
    validateString(jobId)

    ctx.root.redis.zscore(ctx.queueKey, jobId)(function (err, time) {
      if (err) throw err
      if (time) return new Job(ctx.name, jobId, time)

      return this.hget(ctx.activeQueueKey, jobId)(function (err, time) {
        if (err) throw err
        return time ? new Job(ctx.name, jobId, time, true) : null
      })
    })(done)
  })
}

Queue.prototype.deljob = function (jobId) {
  var args = slice(arguments)

  return thunk.call(this, function (done) {
    var ctx = this

    for (var i = 0, l = args.length || 1; i < l; i++) validateString(args[i])

    args.unshift(ctx.queueKey)
    ctx.root.redis.zrem(args)(function (err, count) {
      if (err) throw err
      if (count) return count
      args[0] = ctx.activeQueueKey
      return this.hdel(args)
    })(done)
  })
}

Queue.prototype.ackjob = function (jobId) {
  var args = slice(arguments)
  return thunk.call(this, function (done) {

    for (var i = 0, l = args.length || 1; i < l; i++) validateString(args[i])

    args.unshift(this.queueKey)
    return this.root.redis.hdel(args)(done)
  })
}

Queue.prototype.len = function () {
  return thunk.call(this, function (done) {
    return this.root.redis.zcard(this.queueKey)(done)
  })
}

Queue.prototype.scan = function (count) {}

Queue.prototype._scan = function (count) {}

function Job (queueName, jobId, time, isActive) {
  this.queueName = queueName
  this.jobId = jobId
  this.time = +time
  this.isActive = !!isActive
}

function slice (args, start) {
  start = start || 0
  if (start >= args.length) return []
  var len = args.length
  var ret = Array(len - start)
  while (len-- > start) ret[len - start] = args[len]
  return ret
}

function validateString (str) {
  if (!str || typeof str !== 'string') throw new TypeError(String(str) + ' is invalid string')
}
