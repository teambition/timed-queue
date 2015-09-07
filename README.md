timed-queue
====
Distributed timed job queue, backed by redis.

[![NPM version][npm-image]][npm-url]
[![Build Status][travis-image]][travis-url]

![timed-queue](https://raw.githubusercontent.com/teambition/timed-queue/master/docs/timed-queue.jpg)

## Demo

```js
var TimedQueue = require('timed-queue')
var timedQueue = new TimedQueue({prefix: 'TQ1', interval: 1000 * 60})

timedQueue.connect([7000, 7001, 7002])
  .on('error', function (error) {
    console.error(error)
  })

var eventQueue = timedQueue.queue('event')
eventQueue.on('job', function (job) {
  // ... just do some thing
  eventQueue.ackjob(job.job)()
})

// ...
eventQueue.addjob(eventObj.id, new Date(eventObj.startDate).getTime() - 10 * 60 * 1000)(function (err, res) {
  console.log(err, res)
})
```


## Installation

```bash
npm install timed-queue
```

## API

```js
var TimedQueue = require('timed-queue')
```

### new TimedQueue([options]) => `timedQueue` object

Return a `timedQueue` client. It is an EventEmitter instance.

- `options.prefix`: {String} Redis key's prefix, or namespace. Default to `"TIMEDQ"`
- `options.count`: {Number} The maximum job count for queue's `getjobs` method. Default to `64`
- `options.interval`: {Number} Interval time for scanning. Default to `1000 * 120` ms
- `options.retry`: {Number} Retry time for a job. A job that has been actived but has not been ACK in `retry` time will be actived again. Default to `interval / 2` ms
- `options.expire`: {Number} Expiration time for a job. A job that has been actived and has not been ACK in `expire` time will be removed from the queue. Default to `interval * 5` ms
- `options.accuracy`: {Number} Scanning accuracy. Default to `interval / 5`
- `options.autoScan`: {Boolean} The flag to enable or disable automatic scan. Default to `true`. It can be set to `false` if automatic scan is not desired. Default to `true`

```js
var timedQueue = new TimedQueue()
```

### TimedQueue.prototype.connect([host, options]) => `this`

Connect to redis. Arguments are the same as [thunk-redis](https://github.com/thunks/thunk-redis)'s `createClient`

```js
timedQueue.connect()
```

### TimedQueue.prototype.queue(queue[, options]) => `Queue` instance

Return a `queue` object if one exists. Otherwise it creates a `queue` object and return it. It is a EventEmitter instance.

- `queue`: {String} The queue's name
- `options.count`: {Number} The maximum job count for queue's `getjobs` method. Default to timedQueue's `count`
- `options.retry`: {Number} Retry time for a job. A job that has been actived and has not been ACK in `retry` time will be actived again. Default to timedQueue's `retry`
- `options.expire`: {Number} Expiration time for job. A job that has been actived and has not been ACK in `expire` time will be removed from the queue. Default to timedQueue's `expire`
- `options.accuracy`: {Number} Scanning accuracy, Default to timedQueue's `accuracy`

```js
var eventQueue = timedQueue.queue('event', {retry: 1000, expire: 5000})
```

### TimedQueue.prototype.destroyQueue(queue[, options]) => `this`

Remove the queue. It deletes all data in the queue from redis.

### TimedQueue.prototype.scan() => `this`

Start scanning. It automatically starts after `connect` method is called unless `autoScan` is set to `false`.

### TimedQueue.prototype.stop() => `this`

Stop scanning.

### TimedQueue.prototype.close() => `this`

Close the `timedQueue`. It closes redis client of the `timedQueue` accordingly.

### TimedQueue.prototype.regulateFreq(factor) => `this`

It is used to regulate the automatic scanning frequency.

### Queue.prototype.init([options]) => `this`

- `options.count`: {Number} The maximum job count for queue's `getjobs` method. Default to timedQueue's `count`
- `options.retry`: {Number} Retry time for a job. A job that has been actived and has not been ACK in `retry` time will be actived again. Default to timedQueue's `retry`
- `options.expire`: {Number} Expire time for a job. A job that has been actived and has not been ACK in `expire` time will be removed from queue. Default to timedQueue's `expire`
- `options.accuracy`: {Number} Scanning accuracy. Default to timedQueue's `accuracy`

### Queue.prototype.addjob(job, timing[, job, timing, ...]) => `thunk` function

Add one or more jobs to the queue. It can be used to update the job's timing.

- `job`: {String} The job's name
- `timing`: {Number} The time in millisecond before the job is actived. It should greater than `Date.now()`

```js
eventQueue.addjob('52b3b5f49c2238313600015d', 1441552050409)(function (err, res) {
  console.log(err, res)
  // null, 1
})
```

### Queue.prototype.show(job) => `thunk` function

Show the job info.

- `job`: {String} job

```js
eventQueue.show('52b3b5f49c2238313600015d')(function (err, res) {
  console.log(err, res)
  // {
  //   queue: 'event',
  //   job: '52b3b5f49c2238313600015d',
  //   timing: 1441552050409
  //   active: 0,
  //   retryCount: 0
  // }
})
```

### Queue.prototype.deljob(job[, job, ...]) => `thunk` function

Delete one or more jobs.

- `job`: {String} job

```js
eventQueue.deljob('52b3b5f49c2238313600015d')(function (err, res) {
  console.log(err, res) // null, 1
})
```

### Queue.prototype.getjobs([scanActive]) => `thunk` function

It is called by `Queue.prototype.scan`. It should not be called explicitly.

### Queue.prototype.ackjob(job) => `thunk` function

ACK the job.

- `job`: {String} job

```js
eventQueue.ackjob('52b3b5f49c2238313600015d')(function (err, res) {
  console.log(err, res) // null, 1
})
```

### Queue.prototype.scan() => `thunk` function

It is called by `TimedQueue.prototype.scan`. It should not be called explicitly.

### Queue.prototype.len() => `thunk` function

Return the queue' length.

```js
eventQueue.len()(function (err, res) {
  console.log(err, res) // null, 3
})
```

### Queue.prototype.showActive() => `thunk` function

Return actived jobs in the queue.

```js
eventQueue.showActive()(function (err, res) {
  console.log(err, res) // null, [jobs...]
})
```

### Events

#### timedQueue.on('connect', function () {})
#### timedQueue.on('error', function (error) {})
#### timedQueue.on('close', function () {})
#### timedQueue.on('scanStart', function (queuesLength) {})
#### timedQueue.on('scanEnd', function (queuesLength, timeConsuming) {})

#### queue.on('job', function (job) {})


[npm-url]: https://npmjs.org/package/timed-queue
[npm-image]: http://img.shields.io/npm/v/timed-queue.svg

[travis-url]: https://travis-ci.org/teambition/timed-queue
[travis-image]: http://img.shields.io/travis/teambition/timed-queue.svg
