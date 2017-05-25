/* eslint-env mocha */
'use strict'

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect

const Memdown = require('memdown')
const each = require('async/each')

const IPFSLevel = require('../')
const createRepo = require('./utils/create-repo')

const PARTITION = 'sync-test'

describe('sync', () => {
  const repos = []
  let db1, db2
  const options = {
    log: Memdown(PARTITION)
  }

  before((done) => {
    const repo = createRepo()
    repos.push(repo)
    db1 = IPFSLevel(PARTITION, Object.assign({
      repo: repo
    }, options))
    db1.open(done)
  })

  before((done) => {
    const repo = createRepo()
    repos.push(repo)
    db2 = IPFSLevel(PARTITION, Object.assign({
      repo: repo
    }, options))
    db2.open(done)
  })

  after((done) => setTimeout(done, 4000))

  after((done) => db1.close(done))
  after((done) => db2.close(done))
  after((done) => each(repos, (repo, cb) => repo.destroy(cb), done))

  it('can', (done) => {
    done()
  })
})
