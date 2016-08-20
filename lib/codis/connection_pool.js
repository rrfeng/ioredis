'use strict';

var util = require('util');
var utils = require('../utils');
var EventEmitter = require('events').EventEmitter;
var _ = require('lodash');
var Redis = require('../redis');

function ConnectionPool(redisOptions) {
  EventEmitter.call(this);
  this.redisOptions = redisOptions;

  this.nodes = {};
  this.activeNodes = {};

  this.specifiedOptions = {};
}

util.inherits(ConnectionPool, EventEmitter);

/**
 * Reset the pool with a set of proxies.
 * The old node will be removed.
 *
 * @param {Object[]} nodes
 * @public
 */
ConnectionPool.prototype.reset = function (proxies) {
  var newNodes = _.reduce(proxies, function(result, proxy) {
    result[proxy] = addr2hostport(proxy);
    return result;
  }, {});

  var _this = this;
  _.keys(this.nodes).forEach(function(key) {
    if (!newNodes[key]) {
      _this.nodes[key].disconnect();
    }
  });
  _.keys(newNodes).forEach(function(key) {
    _this.findOrCreate(newNodes[key]);
  });
};

/**
 * Find or create a connection to the node
 *
 * @param {Object} node - the node to connect to
 * @return {Redis}
 * @public
 */
ConnectionPool.prototype.findOrCreate = function (node) {
  setKey(node);

  if (this.specifiedOptions[node.key]) {
    _.assign(node, this.specifiedOptions[node.key]);
  } else {
    this.specifiedOptions[node.key] = node;
  }

  var redis;
  if (this.nodes[node.key]) {
    redis = this.nodes[node.key];
  } else if (this.activeNodes[node.key]) {
    redis = this.activeNodes[node.key];
  } else {
    redis = new Redis(_.defaults({
      retryStrategy: null,
    }, node, this.redisOptions));
    this.nodes[node.key] = redis;

    var _this = this;
    redis.once('end', function () {
      if (_this.activeNodes[node.key]) {
        delete _this.activeNodes[node.key];
      }
      delete _this.nodes[node.key];
      _this.emit('-node', redis);
      if (!Object.keys(_this.nodes).length) {
        _this.emit('drain');
      }
    });

    this.emit('+node', redis);

    redis.on('error', function (error) {
      _this.emit('nodeError', error);
    });

    redis.info(function(err, info) {
      if (err) {
        return redis.emit('error', err);
      }
      _this.activeNodes[node.key] = redis;
      _this.emit('redis active');
    });
  }

  return redis;
};

/**
 * TODO
 */
function addr2hostport(addr) {
  let tmp = addr.split(':');
  return {
    host: tmp[0],
    port: tmp[1],
    key: addr
  };
}

/**
 * Set key property
 *
 * @private
 */
function setKey(node) {
  node = node || {};
  node.port = node.port || 6379;
  node.host = node.host || '127.0.0.1';
  return node;
}

module.exports = ConnectionPool;
