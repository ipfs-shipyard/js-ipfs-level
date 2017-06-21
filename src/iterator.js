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
  constructor (db, ipfs, log, _options, onEnd) {
    super(db)
    this._options = _options || {}
    this._log = log
    this._ipfs = ipfs
    this._onEnd = onEnd
    this._limit = this._options.limit
    this._iterator = null
    if (typeof this._limit !== 'number') {
      this._limit = -1
    }
    this._paused = true
    this._onResumes = []
    this._ipfsIteratorEnded = false

    this._iterator = this._log.iterator(logIteratorOptions(this._options))
  }

  resume () {
    this._paused = false
    const onResumes = this._onResumes
    this._onResumes = []
    onResumes.forEach((fn) => fn())
  }

  _next (done) {
    if (this._paused) {
      this._onResumes.push(this._next.bind(this, done))
      return
    }

    this._iterator.next((err, _key, cid) => {
      if (err) {
        done(err)
        return // early
      }

      waterfall(
        [
          (callback) => {
            if (!_key) {
              this._ipfsIteratorEnded = true
              done()
              this._onceEnded()
              return // early
            }

            if (this._limit === 0) {
              if (!this._ipfsIteratorEnded) {
                this._end()
              }
              done()
              this._onceEnded()
              return
            }

            let key = _key

            if (typeof key !== 'string') {
              key = (key.toString && _key.toString()) || ''
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
            this._ipfs.dag.get(logEntry.cid, (err, result) => callback(err, key, result && result.value))
          },
          (key, value, callback) => {
            if (this._options.valueAsBuffer && !Buffer.isBuffer(value)) {
              value = Buffer.from(String(value))
            }
            if (this._options.keyAsBuffer && !Buffer.isBuffer(key)) {
              key = Buffer.from(String(key))
            }
            if (this._limit > 0) {
              this._limit--
            }
            callback(null, key, value)
          }
        ],
        done)
    })
  }

  _end (callback) {
    if (!this._ipfsIteratorEnded) {
      this._ipfsIteratorEnded = true
      try {
        this._iterator.end(callback)
      } catch (err) {
        // TODO: WGAT TO DO HERE?
        if (callback) {
          callback()
        }
      }
    } else {
      if (callback) {
        callback()
      }
    }
  }

  _onceEnded () {
    if (this._onEnd) {
      this._onEnd()
      this._onEnd = null
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
