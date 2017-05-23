'use strict'

exports.kv = (key, value, options) => {
  return {
    key: key,
    value: value
  }
}

exports.deleted = (key) => {
  return {
    key: key,
    deleted: true
  }
}
