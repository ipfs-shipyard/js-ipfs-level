'use strict'

const merge = require('deep-assign')
const AbstractIterator = require('abstract-leveldown').AbstractIterator

const MANDATORY_OPTIONS = {
  valueAsBuffer: false
}

module.exports = class Iterator extends AbstractIterator {
  constructor (db, ipfs, heads, _options) {
    super(db)
    const options = merge({}, _options || {}, MANDATORY_OPTIONS)
    this._iterator = heads.iterator(options)
    this._ipfs = ipfs
  }

  _next (callback) {
    this._iterator.next((err, key, value) => {
      if (err) {
        callback(err)
        return // early
      }

      if (!key) {
        callback()
        return // early
      }

      this._ipfs.dag.get(value, (err, result) => {
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
