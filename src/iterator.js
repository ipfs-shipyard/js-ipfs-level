'use strict'

const merge = require('deep-assign')
const AbstractIterator = require('abstract-leveldown').AbstractIterator
const waterfall = require('async/waterfall')

const decoding = require('./decoding')

const MANDATORY_LOG_OPTIONS = {
  valueAsBuffer: false
}

const KEY_PREFIX_GT = 'key:'
const KEY_PREFIX_LT = 'key;'

module.exports = class Iterator extends AbstractIterator {
  constructor (db, ipfs, log, _options) {
    super(db)
    this._options = _options || {}
    this._log = log
    this._iterator = log.iterator(logIteratorOptions(this._options))
    this._ipfs = ipfs
    this._limit = this._options.limit
    if (typeof this._limit !== 'number') {
      this._limit = -1
    }
    this._ended = false
  }

  _next (done) {
    waterfall(
      [
        (callback) => this._iterator.next((err, key, cid) => callback(err, key, cid)),
        (_key, cid, callback) => {
          if (!_key) {
            done()
            return // early
          }

          if (this._limit === 0) {
            if (!this._ended) {
              this._ended = true

              // TODO: because _iterator.end may have been called (???)
              try {
                this._iterator.end(() => {})
              } catch (err) {
                // TODO: NOTHING??
              }
              done()
            } else {
              done()
            }
            return
          }

          let key = _key

          if (typeof key !== 'string') {
            key = key.toString && _key.toString() || ''
          }

          if (key.indexOf(KEY_PREFIX_GT) !== 0) {
            this._next(done)
            return // early
          }

          key = key.substring(KEY_PREFIX_GT.length)


          this._log.get('cid:' + cid, decoding((err, logEntry) => callback(err, key, logEntry)))
        },
        (key, logEntry, callback) => {
          if (!logEntry || logEntry.deleted) {
            this._next(done)
            return // early
          }
          this._ipfs.block.get(logEntry.cid, (err, result) => callback(err, key, result && result.data))
        },
        (key, value, callback) => callback(null, key, JSON.parse(value)),
        (key, value, callback) => {
          if (this._options.valueAsBuffer && !Buffer.isBuffer(value)) {
            value = new Buffer(String(value))
          }
          if (this._options.keyAsBuffer && !Buffer.isBuffer(key)) {
            key = new Buffer(String(key))
          }
          if (this._limit > 0) {
            this._limit--
          }
          callback(null, key, value)
        }
      ],
      done)
  }

  _end (callback) {
    if (!this._ended) {
      this._ended = true
      try {
        this._iterator.end(callback)
      } catch (err) {
        // TODO: WGAT TO DO HERE?
        callback()
      }
    } else {
      callback()
    }
  }
}

function logIteratorOptions (_options) {
  const options = merge({}, _options || {}, MANDATORY_LOG_OPTIONS)
  if (options.gt) {
    options.gt = KEY_PREFIX_GT + options.gt
  }
  if (options.gte) {
    options.gte = KEY_PREFIX_GT + options.gte
  }
  if (options.lt) {
    options.lt = KEY_PREFIX_GT + options.lt
  }
  if (options.lte) {
    options.lte = KEY_PREFIX_GT + options.lte
  }
  if (options.start) {
    options.start = KEY_PREFIX_GT + options.start
  }
  if (options.end) {
    options.end = KEY_PREFIX_GT + options.end
  }

  if (!options.gt && !options.gte && !options.start) {
    options.start = _options.reverse ? KEY_PREFIX_LT : KEY_PREFIX_GT
  }
  if (!options.lt && !options.lte && !options.end) {
    options.end = _options.reverse ? KEY_PREFIX_GT : KEY_PREFIX_LT
  }
  // don't limit
  options.limit = -1

  return options
}