# ipfs-level

> Leveldown over IPFS


[![made by Protocol Labs](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](https://protocol.io)
[![Freenode](https://img.shields.io/badge/freenode-%23ipfs-blue.svg?style=flat-square)](http://webchat.freenode.net/?channels=%23ipfs)

[![Build Status](https://travis-ci.org/ipfs-shipyard/ipfs-level.svg?branch=master)](https://travis-ci.org/ipfs-shipyard/ipfs-level)

# Install

```bash
$ npm install ipfs-level --save
```

# Import

```js
const IPFSLevel = require('ipfs-level')
```

# API

## IPFSLevel(partition, options)

Returns a new IPFSLevel instance. This object obeys [the LevelDown interface](https://github.com/level/leveldown).

Arguments:

* `options` (object, defaults to [this](src/default-options.js)): with the following keys:
  * `ipfsOptions` (object). [IPFS options object](https://github.com/ipfs/js-ipfs#advanced-options-when-creating-an-ipfs-node).
  * `log` (LevelDown-compatible database that stores the log)
  * `ipfs` (IPFS object): an IPFS object instance. If you already can provide an IPFS object, pass it in here.
  * `retainLog` (boolean, defaults to false`): whether or not the log should be retained. Besides wasting storage space, setting this option to `true` provides no direct benefit, except if you want to somehow explore the log database.

# Default arguments

You can create a constructor that curries some default arguments by using `IPFSLevel.defaults(options)` like this:

```js
const ipfsLevel = IPFSLevel.defaults({
  log: someLevelDownLogDatabase
})
```

## Events

An IPFSLevel instance emits the following events

### `emit("started")`

When started.

### `emit("change", change)`

Emitted whenever there is a change in the database. The event payload is a `change` object, which has the following properties:
  * `type` (string: "put" or "del")
  * `key` (string): the key affected by this change
  * `value` (any): the new value for the mentioned key

### `emit("new head", cid)`

Whenever the log has a new head. The payload, `cid` is a content identifier (internal to IPFS)


## With Levelup


This default options feature may be useful if you want to pass a constructor into which you'll have no saying about the options, like on the Levelup constructor:

```js
const LevelUp = require('levelup')
const Memdown = require('memdown') // any leveldown db will do for caching log entries
const IPFSLevel = require('ipfs-level').defaults({
  log: Memdown('some-partition-name') // log database should be scoped to partition
})

const db = LevelUp({ db: IPFSLevel })
// now you have a levelup db you can use
```

# Test and debug

This package uses [debug](https://github.com/visionmedia/debug#readme), so you can activate debug messages by setting the environment variable `DEBUG` to `ipfs-level:*`

# License

MIT

## Contribute

Feel free to join in. All welcome. Open an [issue](https://github.com/pgte/ipfs-level/issues)!

This repository falls under the IPFS [Code of Conduct](https://github.com/ipfs/community/blob/master/code-of-conduct.md).

[![](https://cdn.rawgit.com/jbenet/contribute-ipfs-gif/master/img/contribute.gif)](https://github.com/ipfs/community/blob/master/contributing.md)

## License

[MIT](LICENSE)
