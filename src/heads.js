'use strict'

const setImmediate = require('async/setImmediate')

// TODO: persist heads

class Heads {
  constructor (partition) {
    this._partition = partition
    this._heads = {}
  }

  set (key, cid, callback) {
    this._heads[key] = cid
    setImmediate(callback)
  }

  get (key, callback) {
    setImmediate(() => callback(null, this._heads[key]))
  }
}

module.exports = (partition) => new Heads(partition)
