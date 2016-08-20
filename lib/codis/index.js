'use strict';

var Promise = require('bluebird');
var Deque = require('double-ended-queue');
var Redis = require('../redis');
var utils = require('../utils');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var debug = require('debug')('ioredis:cluster');
var _ = require('lodash');
var ScanStream = require('../scan_stream');
var Commander = require('../commander');
var Command = require('../command');
var commands = require('redis-commands');
var ConnectionPool = require('./connection_pool');

var TYPES = {
    NODE_CREATED : 1,
    NODE_DELETED : 2,
    NODE_DATA_CHANGED : 3,
    NODE_CHILDREN_CHANGED : 4
};
var RESET_TYPES = _.values(TYPES);

/**
 * Creates a Redis Codis instance
 *
 * @constructor
 * @param {Object} options
 * @param {function} [options.codisRetryStrategy] - See "Quick Start" section
 * @param {boolean} [options.enableOfflineQueue=true] - See Redis class
 * @param {Object} [options.redisOptions] - Passed to the constructor of `Redis`.
 * @param {Object} [options.zkOptions] - Passed to the constructor of `Redis`.
 * @extends [EventEmitter](http://nodejs.org/api/events.html#events_class_events_eventemitter)
 * @extends Commander
 */
function Codis(options) {
  EventEmitter.call(this);
  Commander.call(this);
  this.options = options;


  this.connectionPool = new ConnectionPool(this.options.redisOptions);

  var _this = this;
  this.connectionPool.on('-node', function (redis) {
    _this.emit('-node', redis);
  });
  this.connectionPool.on('+node', function (redis) {
    _this.emit('+node', redis);
  });
  this.connectionPool.on('drain', function () {
    _this.setStatus('close');
  });
  this.connectionPool.on('nodeError', function (error) {
    _this.emit('node error', error);
  });

  this.resetOfflineQueue();

  if (this.options.lazyConnect) {
    this.setStatus('wait');
  } else {
    this.connect().catch(_.noop);
}
util.inherits(Cluster, EventEmitter);
_.assign(Cluster.prototype, Commander.prototype);

Codis.prototype.resetOfflineQueue = function () {
  this.offlineQueue = new Deque();
};

/**
 * Connect to a codis?
 *
 * @return {Promise}
 * @public
 */
Codis.prototype.connect = function () {
  function readyHandler() {
    this.setStatus('ready');
    this.retryAttempts = 0;
    this.executeOfflineCommands();
  }

  return new Promise(function (resolve, reject) {
    if (this.status === 'connecting' || this.status === 'connect' || this.status === 'ready') {
      reject(new Error('Redis is already connecting/connected'));
      return;
    }
    this.setStatus('connecting');

    var closeListener;
    var activeListener = function () {
      this.removeListener('close', closeListener);
      this.manuallyClosing = false;
      this.setStatus('connect');
      readyHandler.call(this);
      resolve();
    };

    closeListener = function () {
      this.removeListener('refresh', refreshListener);
      reject(new Error('None of startup nodes is available'));
    };

    this.once('close', closeListener);
    this.once('close', this._handleCloseEvent.bind(this));

    this.connectionPool.once('redis active', activeListener) // TODO 这里是 once 还是 on 好..

    var _this = this;
    return this._initZK().then(function(proxies) {
      _this.connectionPool.reset(proxies);
      _this.checkCodisProxy(function (err) {

      })
    });

  }.bind(this));
};

/**
 * init zookeeper and listener
 *
 */
Codis.prototype._initZK = function initZK() {
  if (this.zkClient) return;

  var _this = this;
  var path = this.options.zkOptions.path;
  var zk = new ZooKeeper({
    connect: this.options.zkOptions.host,
    timeout: this.options.zkOptions.timeout
  });

  var getProxies = function(nodes) {
    return Promise.map(nodes, function(node) {
      var nodePath = path + '/' + node;
      return getData(_this.zkClient, nodePath);
    });
  };

  return new Promise(function(resolve, reject) {

    var resetConnectionPool = function(rc, error, children) {
      if (rc) { return reject(rc); }
      return getProxies(children).then(function(proxies) {
        _this.connectionPool.reset(proxies);
      });
    };

    var wCallback = function(type, state, path_w) {
      if (_.includes(RESET_TYPES, type)) {
        _this.zkClient.a_get_children(path, false, resetConnectionPool);
      }
    };

    zk.connect(function(err, client) {
      _this.zkClient = client;
      client.aw_get_children(path, wCallback, function(rc, error, children) {
        if (rc) { return reject(rc); }
        resolve(getProxies(children));
      });
    });
  });
}

/**
 * Called when closed to check whether a reconnection should be made
 */
Codis.prototype._handleCloseEvent = function () {
  var retryDelay;
  if (!this.manuallyClosing && typeof this.options.codisRetryStrategy === 'function') {
    retryDelay = this.options.codisRetryStrategy.call(this, ++this.retryAttempts);
  }
  if (typeof retryDelay === 'number') {
    this.setStatus('reconnecting');
    this.reconnectTimeout = setTimeout(function () {
      this.reconnectTimeout = null;
      debug('Cluster is disconnected. Retrying after %dms', retryDelay);
      this.connect().catch(_.noop);
    }.bind(this), retryDelay);
  } else {
    this.setStatus('end');
    this.flushQueue(new Error('None of startup nodes is available'));
  }
};

/**
 * Disconnect from every node in the cluster.
 *
 * @public
 */
Codis.prototype.disconnect = function (reconnect) {
  var status = this.status;
  this.setStatus('disconnecting');

  if (!reconnect) {
    this.manuallyClosing = true;
  }
  if (this.reconnectTimeout) {
    clearTimeout(this.reconnectTimeout);
    this.reconnectTimeout = null;
  }

  if (status === 'wait') {
    this.setStatus('close');
    this._handleCloseEvent();
  } else {
    this.connectionPool.reset([]);
  }
};

/**
 * Quit the cluster gracefully.
 *
 * @param {function} callback
 * @return {Promise} return 'OK' if successfully
 * @public
 */
Codis.prototype.quit = function (callback) {
  var status = this.status;
  this.setStatus('disconnecting');

  this.manuallyClosing = true;

  if (this.reconnectTimeout) {
    clearTimeout(this.reconnectTimeout);
    this.reconnectTimeout = null;
  }
  if (status === 'wait') {
    var ret = Promise.resolve('OK').nodeify(callback);

    // use setImmediate to make sure "close" event
    // being emitted after quit() is returned
    setImmediate(function () {
      this.setStatus('close');
      this._handleCloseEvent();
    }.bind(this));

    return ret;
  }

  return Promise.all(this.nodes().map(function (node) {
    return node.quit();
  })).then(function () {
    return 'OK';
  }).nodeify(callback);
};

/**
 * Change cluster instance's status
 *
 * @param {string} status
 * @private
 */
Codis.prototype.setStatus = function (status) {
  debug('status: %s -> %s', this.status || '[empty]', status);
  this.status = status;
  process.nextTick(this.emit.bind(this, status));
};

/**
 * Get nodes with the specified role
 *
 * @param {string} [role=all] - role, "master", "slave" or "all"
 * @return {Redis[]} array of nodes
 * @public
 */
Codis.prototype.nodes = function () {
  return _.values(this.connectionPool.nodes[role]);
};

/**
 * Flush offline queue with error.
 *
 * @param {Error} error - The error object to send to the commands
 * @private
 */
Codis.prototype.flushQueue = function (error) {
  var item;
  while (this.offlineQueue.length > 0) {
    item = this.offlineQueue.shift();
    item.command.reject(error);
  }
};

Codis.prototype.executeOfflineCommands = function () {
  if (this.offlineQueue.length) {
    debug('send %d commands in offline queue', this.offlineQueue.length);
    var offlineQueue = this.offlineQueue;
    this.resetOfflineQueue();
    while (offlineQueue.length > 0) {
      var item = offlineQueue.shift();
      this.sendCommand(item.command, item.stream, item.node);
    }
  }
};

Codis.prototype.sendCommand = function (command, stream, node) {
  if (this.status === 'wait') {
    this.connect().catch(_.noop);
  }
  if (this.status === 'end') {
    command.reject(new Error(utils.CONNECTION_CLOSED_ERROR_MSG));
    return command.promise;
  }
  var to = this.options.scaleReads;
  if (to !== 'master') {
    var isCommandReadOnly = commands.exists(command.name) && commands.hasFlag(command.name, 'readonly');
    if (!isCommandReadOnly) {
      to = 'master';
    }
  }

  var redis = _.sample(this.connectionPool.activeNodes);

  if (redis) {
    redis.sendCommand(command, stream);
  } else if (_this.options.enableOfflineQueue) {
    this.offlineQueue.push({
      command: command,
      stream: stream
    });
  } else {
    command.reject(new Error('Cluster isn\'t ready and enableOfflineQueue options is false'));
  }
  return command.promise;
};

/**
 * Default options
 *
 * @var defaultOptions
 * @protected
 */
Codis.defaultOptions = {
  codisRetryStrategy: function (times) {
    return Math.min(100 + times * 2, 2000);
  },
  enableOfflineQueue: true
};

function getData(zk, path) {
  return new Promise((resolve, reject) => {
    zk.a_get(path, false, (rc, error, stat, data) => {
      if (rc) {
        return reject(rc);
      }
      try {
        return resolve(JSON.parse(data.toString()));
      } catch(e) {
        return reject(e);
      }
    });
  });
}

['sscan', 'hscan', 'zscan', 'sscanBuffer', 'hscanBuffer', 'zscanBuffer']
.forEach(function (command) {
  Codis.prototype[command + 'Stream'] = function (key, options) {
    return new ScanStream(_.defaults({
      objectMode: true,
      key: key,
      redis: this,
      command: command
    }, options));
  };
});

require('../transaction').addTransactionSupport(Cluster.prototype);

module.exports = Cluster;
