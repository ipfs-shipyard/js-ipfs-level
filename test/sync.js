/* eslint-env mocha */
'use strict'

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect

const Memdown = require('memdown')
const each = require('async/each')
const parallel = require('async/parallel')
const map = require('async/map')

const IPFSLevel = require('../')
const createRepo = require('./utils/create-repo')

const PARTITION = 'sync-test'

const WAIT_FOR_SYNC_MS = 5000

describe('sync', () => {
  const repos = []
  let db1, db2, db3, electedKey3Value

  before((done) => {
    const repo = createRepo()
    repos.push(repo)
    db1 = IPFSLevel(PARTITION, {
      ipfsOptions: {
        repo: repo
      },
      log: Memdown(PARTITION + ':db1')
    })
    db1.open(done)
  })

  before((done) => {
    const repo = createRepo()
    repos.push(repo)
    db2 = IPFSLevel(PARTITION, {
      ipfsOptions: {
        repo: repo
      },
      log: Memdown(PARTITION + ':db2')
    })
    db2.open(done)
  })

  after((done) => each(repos, (repo, cb) => repo.teardown(cb), done))

  it('puts in one', (done) => {
    db1.put('key', 'value', done)
  })

  it('waits a bit', (done) => {
    setTimeout(done, WAIT_FOR_SYNC_MS * 2)
  })

  it('put was replicated', (done) => {
    db2.get('key', (err, result) => {
      expect(err).to.not.exist()
      expect(result).to.equal('value')
      done()
    })
  })

  it('puts some keys', (done) => {
    parallel(
      [
        (callback) => db1.put('key 1', 'value 1', callback),
        (callback) => db2.put('key 2', 'value 2', callback)
      ], done)
  })

  it('waits some', (done) => {
    setTimeout(done, WAIT_FOR_SYNC_MS)
  })

  it('merged', (done) => {
    parallel([
      (callback) => {
        db2.get('key 1', (err, result) => {
          expect(err).to.not.exist()
          expect(result).to.equal('value 1')
          callback()
        })
      },
      (callback) => {
        db1.get('key 2', (err, result) => {
          expect(err).to.not.exist()
          expect(result).to.equal('value 2')
          callback()
        })
      }
    ], done)
  })

  it('concurrent put', (done) => {
    parallel(
      [
        (callback) => db1.put('key 3', 'value 3.1', callback),
        (callback) => db2.put('key 3', 'value 3.2', callback)
      ],
      done
    )
  })

  it('waits some', (done) => {
    setTimeout(done, WAIT_FOR_SYNC_MS)
  })

  it('merged and elected one value', (done) => {
    const results = []
    parallel(
      [
        (callback) => {
          db2.get('key 3', (err, result) => {
            expect(err).to.not.exist()
            results.push(result)
            callback()
          })
        },
        (callback) => {
          db1.get('key 3', (err, result) => {
            expect(err).to.not.exist()
            results.push(result)
            callback()
          })
        }
      ],
      (err) => {
        if (err) {
          return done(err)
        }
        expect(results.length).to.equal(2)
        expect(results[0]).to.equal(results[1])
        electedKey3Value = results[0]
        expect(electedKey3Value).to.exist()
        done()
      })
  })

  it('a third node appears', (done) => {
    const repo = createRepo()
    repos.push(repo)
    db3 = IPFSLevel(PARTITION, {
      ipfsOptions: {
        repo: repo
      },
      log: Memdown(PARTITION + ':db3')
    })
    db3.open(done)
  })

  it('waits a bit', (done) => {
    setTimeout(done, WAIT_FOR_SYNC_MS * 2)
  })

  it('has all values', (done) => {
    map(
      ['1', '2', '3'],
      (key, callback) => {
        db3.get('key ' + key, (err, result) => {
          expect(err).to.not.exist()
          callback(null, result)
        })
      },
      (err, results) => {
        expect(err).to.not.exist()
        expect(results).to.deep.equal(['value 1', 'value 2', electedKey3Value])
        done()
      })
  })
})
