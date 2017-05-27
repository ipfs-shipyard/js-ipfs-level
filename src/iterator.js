'use strict'

const merge = require('deep-assign')
const AbstractIterator = require('abstract-leveldown').AbstractIterator
const waterfall = require('async/waterfall')

const decoding = require('./decoding')

const MANDATORY_OPTIONS = {
  valueAsBuffer: false
}

module.exports = class Iterator extends AbstractIterator {
  constructor (db, ipfs, log, _options) {
    super(db)
    const options = merge({}, _options || {}, MANDATORY_OPTIONS)
    this._log = log
    this._iterator = log.iterator(options)
    this._ipfs = ipfs
  }

  _next (done) {
    waterfall(
      [
        (callback) => this._iterator.next((err, key, cid) => callback(err, key, cid)),
        (_key, cid, callback) => {
          console.log('KEY:', _key)
          if (! _key) {
            done()
            return // early
          }

          // skip this one if it's not a key
          // TODO: patch the option limits to only get relevant keys
          if (_key.indexOf('key:') !== 0) {
            this._next(done)
            return // early
          }
          const key = _key.substring(4)

          this._log.get('cid:' + cid, decoding((err, logEntry) => callback(err, key, logEntry)))
        },
        (key, logEntry, callback) => {
          console.log('LOG entry:', logEntry)
          this._ipfs.dag.get(logEntry.cid, (err, result) => callback(err, key, result))
        },
        (key, result, callback) => callback(null, key, result && result.value.value)
      ],
      done)
  }

  _end (callback) {
    this._iterator.end(callback)
  }
}
