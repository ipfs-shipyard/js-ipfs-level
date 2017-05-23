/* eslint-env mocha */
'use strict'

const chai = require('chai')
chai.use(require('dirty-chai'))
// const expect = chai.expect

const IPFSLevel = require('../')

const PARTITION = 'test'

describe('leveldown interface', () => {
  let db

  it('can create leveldown object', (done) => {
    db = IPFSLevel(PARTITION)
    done()
  })

  it('can be opened', (done) => {
    db.open(done)
  })

  it('can put a value', (done) => {
    db.put('key', 'value', done)
  })

  it('can be closed', (done) => {
    db.close(done)
  })
})
