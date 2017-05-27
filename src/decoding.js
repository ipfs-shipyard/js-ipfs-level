'use strict'

function decode (str) {
  if (Buffer.isBuffer(str)) {
    str = str.toString()
  }
  if (typeof str === 'object') {
    return str
  }
  return JSON.parse(str)
}

module.exports = function decoding (callback) {
  return (err, str) => {
    if (err && err.message === 'NotFound') {
      callback(null, undefined)
    } else {
      callback(err, str && decode(str))
    }
  }
}
