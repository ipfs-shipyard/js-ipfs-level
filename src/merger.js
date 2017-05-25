'use strict'

const waterfall = require('async/waterfall')
const series = require('async/series')
const eachSeries = require('async/eachSeries')
const Queue = require('async/queue')
const vectorclock = require('vectorclock')

const decoding = require('./decoding')

module.exports = class Merger {
  constructor (ipfs, log) {
    this._ipfs = ipfs
    this._log = log
    this._queue = Queue(this._processRemoteHead.bind(this), 1)
  }

  processRemoteHead (cid, callback) {
    this._queue.push(cid, callback)
  }

  _processRemoteHead (cid, callback) {
    console.log('processing remote head %j, ...', cid)
    const newLogEntries = []

    const ensureLogEntry = (cid, callback) => {
      this._log.get(cid, (err, entry) => {
        if (err && err.message === 'NotFound') {
          newLogEntries.push(cid)
          waterfall([
            (callback) => this._ipfs.dag.get(cid, callback),
            (logEntry) => {
              series([
                (callback) => this._log.set(cid, encode(logEntry), callback),
                (callback) => {
                  if (logEntry.parent) {
                    ensureLogEntry(logEntry.parent, callback)
                  } else {
                    callback()
                  }
                }
              ], callback)
            }
          ], callback)
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

      const remoteWinners = []

      eachSeries(
        newLogEntries.reverse(),
        (remoteEntryCID, callback) => {
          this._log.get(remoteEntryCID, decoding((err, remoteLogEntry) => {
            if (err) {
              callback(err)
              return // early
            }

            this._log.getLatest(remoteLogEntry.key, (err, localLatestLogEntry) => {
              if (err) {
                callback(err)
                return // early
              }

              switch (vectorclock.compare(localLatestLogEntry, remoteLogEntry)) {
                case -1:
                  // local latest log entry happened BEFORE remote one
                  // TODO
                  remoteWinners.push(remoteEntryCID)
                  this._log.impose(remoteLogEntry, callback)
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
                  // local latest log entry is CONCURRENT to remote one

                  const chosenEntry = chooseOne(localLatestLogEntry, remoteLogEntry)
                  if (chosenEntry === remoteLogEntry) {
                    remoteWinners.push(remoteEntryCID)
                    this._log.impose(remoteLogEntry, callback)
                  } else {
                    // our entry won, ignore remote one
                    callback()
                  }
                  break
              }
            })
          }))
        },
        (err) => {
          if (err) {
            callback(err)
            return // early
          }

          // set HEAD if necessary
          if (remoteWinners.length && remoteWinners[remoteWinners.length - 1] === cid) {
            this._log.get(cid, (err, newHead) => {
              if (err) {
                callback(err)
                return // early
              }

              this._log.setHead(newHead, callback)
            })
          } else {
            callback()
          }
        }
      )
    })
  }
}

function encode (logEntry) {
  return JSON.stringify(logEntry)
}

function chooseOne (a, b) {
  if (a.cid > b.cid) {
    return a
  } else {
    return b
  }
}
