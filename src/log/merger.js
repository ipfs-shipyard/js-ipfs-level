'use strict'

const waterfall = require('async/waterfall')
const series = require('async/series')
const each = require('async/each')
const eachSeries = require('async/eachSeries')
const parallel = require('async/parallel')
const Queue = require('async/queue')
const vectorclock = require('vectorclock')
const debug = require('debug')('ipfs-level:merger')

const decoding = require('../decoding')

module.exports = class Merger {
  constructor (nodeId, ipfs, log) {
    this._nodeId = nodeId
    this._ipfs = ipfs
    this._log = log
    this._headQueue = []
    this._queue = Queue(this._processRemoteHead.bind(this), 1)
  }

  processRemoteHead (cid, callback) {
    if (this._headQueue.indexOf(cid) < 0) {
      debug('will process remote head %j, ...', cid)
      this._headQueue.push(cid)
      this._queue.push(cid, (err) => {
        this._headQueue.splice(this._headQueue.indexOf(cid), 1)
        callback(err)
      })
    } else {
      callback()
    }
  }

  _processRemoteHead (cid, callback) {
    debug('processing remote head %j, ...', cid)
    const newLogEntries = []

    const ensureLogEntry = (cid, callback) => {
      if (!cid) {
        throw new Error('need a CID')
      }
      this._log.get(cid, (err, entry) => {
        if (err) {
          callback(err)
          return // early
        }

        debug('log entry in cache: %j', entry)
        if (!entry || entry.isNew) {
          newLogEntries.push(cid)
          waterfall(
            [
              (callback) => this._ipfs.dag.get(cid, callback),
              (logEntry, callback) => {
                const newEntry = Object.assign({}, logEntry.value, { isNew: true })
                series([
                  (callback) => this._log.set(cid, newEntry, callback),
                  (callback) => {
                    if (logEntry.value.parents) {
                      each(
                        logEntry.value.parents,
                        ensureLogEntry,
                        callback)
                    } else {
                      callback()
                    }
                  }
                ], callback)
              }
            ],
            callback)
        } else {
          callback()
        }
      })
    }

    ensureLogEntry(cid, (err) => {
      if (err) {
        callback(err)
        return // early
      }

      debug('all log entries for remote head %s are ensured', cid)
      debug('new log entries:', newLogEntries)

      if (newLogEntries.length) {
        this._log.transaction((callback) => {
          series([
            (callback) => this._processNewRemoteLogEntries(cid, newLogEntries, callback),
            (callback) => this._mergeHeads(cid, callback)
          ], callback)
        }, callback)
      } else {
        callback()
      }
    })
  }

  _processNewRemoteLogEntries (remoteHeadCID, newLogEntries, callback) {
    newLogEntries = newLogEntries.reverse()

    debug('process new remote log entries: %j', newLogEntries)
    series([
      (callback) => {
        eachSeries(
          newLogEntries,
          (remoteEntryCID, callback) => {
            debug('processing remote log entry %s ...', remoteEntryCID)
            debug('trying to retrieve remote log entry from local cache')
            this._log.get(remoteEntryCID, decoding((err, remoteLogEntry) => {
              if (err) {
                debug('error trying to retrieve remote log entry from local cache:', err)
                callback(err)
                return // early
              }

              if (!remoteLogEntry) {
                debug('remote log entry %s was NOT found in cache', remoteEntryCID)
              }

              debug('processing new remote log entry for key %s: %j', remoteLogEntry.key, remoteLogEntry)

              if (!remoteLogEntry.key) {
                callback()
                return // early
              }

              this._log.getLatest(remoteLogEntry.key, (err, localLatestLogEntry) => {
                if (err) {
                  callback(err)
                  return // early
                }

                debug('local log entry for key %s is %j', remoteLogEntry.key, localLatestLogEntry)

                const compared = localLatestLogEntry
                  ? vectorclock.compare(localLatestLogEntry, remoteLogEntry)
                  : -1

                switch (compared) {
                  case -1:
                    // local latest log entry happened BEFORE remote one
                    // TODO
                    this._log.impose(remoteLogEntry.key, remoteEntryCID, callback)
                    break
                  case 1:
                    // local latest log entry happened AFTER remote one
                    // remote log entry is outdated, ignore it
                    callback()
                    break
                  case 0:
                    if (vectorclock.isIdentical(localLatestLogEntry, remoteLogEntry)) {
                      // local latest log entry is IDENTICAL to the remote one
                      callback()
                      return // early
                    }
                    debug('conflict for key %j', remoteLogEntry.key)
                    // local latest log entry is CONCURRENT to remote one

                    const chosenEntry = chooseOne(localLatestLogEntry, remoteLogEntry)
                    if (chosenEntry === remoteLogEntry) {
                      this._log.impose(remoteLogEntry.key, remoteEntryCID, callback)
                    } else {
                      // our entry won, ignore remote one
                      callback()
                    }
                }
              })
            }))
          },
          callback
        )
      },
      (callback) => {
        debug('going to mark new log entries as visited: %j', newLogEntries)
        eachSeries(
          newLogEntries,
          (entryCID, callback) => {
            waterfall([
              (callback) => this._log.get(entryCID, callback),
              (entry, callback) => {
                delete entry.isNew
                callback(null, entry)
              },
              (entry, callback) => this._log.set(entryCID, entry, callback)
            ], callback)
          },
          callback)
      }
    ], callback)
  }

  _mergeHeads (remoteHeadCID, callback) {
    debug('going to determine if setting HEAD is required...', remoteHeadCID)
    parallel(
      {
        localCID: (callback) => this._log.getLatestHeadCID(callback),
        local: (callback) => this._log.getLatestHead(callback),
        remote: (callback) => this._log.get(remoteHeadCID, callback)
      },
      (err, results) => {
        if (err) {
          callback(err)
          return // early
        }

        if (!results.localCID) {
          this._log.setHeadCID(remoteHeadCID, callback)
          return // early
        }

        if (results.localCID === remoteHeadCID) {
          debug('heads %j and %j are the same')
          callback()
          return // early
        }

        const parents = [remoteHeadCID, results.localCID].sort()

        const mergeHead = {
          parents: parents,
          clock: vectorclock.merge(results.local.clock, results.remote.clock)
        }

        this._log.setHead(mergeHead, callback)
      }
    )
  }
}

function chooseOne (a, b) {
  if (a.cid > b.cid) {
    return a
  } else {
    return b
  }
}
