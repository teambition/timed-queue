'use strict'

var gulp = require('gulp')
var mocha = require('gulp-mocha')
var gulpSequence = require('gulp-sequence')

gulp.task('mocha', function () {
  return gulp.src('test/*.js', {read: false})
    .pipe(mocha({
      timeout: 60000
    }))
})

gulp.task('exit', function (callback) {
  callback()
  process.exit(0)
})

gulp.task('default', gulpSequence('test'))
gulp.task('test', gulpSequence('mocha', 'exit'))
