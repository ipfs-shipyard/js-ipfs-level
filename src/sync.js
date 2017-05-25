'use strict'

module.exports = class Sync {
  constructor (partition, log, ipfs) {
    this._topic = '/ipfs-level/' + partition
    this._log = log
    this._ipfs = ipfs
    this._head = undefined
  }

  setNewHead (head) {
    this._head = head
  }
}
