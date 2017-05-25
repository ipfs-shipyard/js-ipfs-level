'use strict'

const merge = require('deep-assign')
const AbstractIterator = require('abstract-leveldown').AbstractIterator

const MANDATORY_OPTIONS = {
  valueAsBuffer: false
}

module.exports = class Iterator extends AbstractIterator {
  constructor (db, ipfs, log, _options) {
    super(db)
    const options = merge({}, _options || {}, MANDATORY_OPTIONS)
    this._iterator = log.iterator(options)
    this._ipfs = ipfs
  }

  _next (callback) {
    this._iterator.next((err, _key, _value) => {
      if (err) {
        callback(err)
        return // early
      }

      if (!_key) {
        callback()
        return // early
      }

      // skip this one if it's not a key
      // TODO: patch the option limits to only get relevant keys
      if (_key.indexOf('key:') !== 0) {
        this._next(callback)
        return // early
      }
      const key = _key.substring(4)
      const value = JSON.parse(_value)

      this._ipfs.dag.get(value.cid, (err, result) => {
        if (err) {
          callback(err)
          return // early
        }

        callback(null, key, result.value && result.value.value)
      })
    })
  }

  _end (callback) {
    this._iterator.end(callback)
  }
}
