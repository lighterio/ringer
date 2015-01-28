var net = require('net');
var Flagger = require('../common/event/flagger');
var Errors = require('./errors');
var Events = require('./events');
require('../common/json/stringify');

/**
 * A Peer is a ring member which normally runs in an external process.
 */
var Peer = module.exports = Flagger.extend({

  /**
   * Create a peer with a host and port.
   */
  init: function (name, client) {
    var self = this;
    var hostAndPort = name.split(':');
    self.name = name;
    self.host = hostAndPort[0];
    self.port = hostAndPort[1];
    self.isLeader = false;
    self.client = client;
    self.logger = client.logger;
    self.connect.delay = client.config.connectDelay;
    self.status = Peer.UNKNOWN;
    self.errors = {};
    self.latency = 100;
    self._events = new Events();
    self.connect();
  },

  /**
   * Connect to this peer so we can send and receive data.
   */
  connect: function () {
    var self = this;

    // On success, declare this peer to be up.
    var now = Date.now();
    var socket = self.socket = net.connect(self.port, self.host, function () {
      self.status = Peer.UP;
      self.emit('socket:connected', socket);
    });

    // Listen for incoming messages.
    self.listen(socket);
  },

  /**
   * Listen for data/errors/hangups on a socket.
   */
  listen: function (socket) {
    var self = this;
    var client = self.client;
    var buffer = '';

    socket.latency = 0;
    socket.queue = [];

    // When data is received, self-emit it.
    socket.on('data', function (chunk) {
      buffer += chunk;
      var messages = buffer.split(/\n/g);
      buffer = messages.pop();
      messages.forEach(function (message) {
        var pos = message.indexOf('~');
        var type, json, data;
        if (pos < 0) {
          type = message;
        }
        else {
          type = message.substr(0, pos);
          json = message.substr(pos + 1);
          try {
            data = JSON.parse(json);
          }
          catch (e) {
            self.logger.error('[Ringer] Invalid JSON: ' + json, typeof json);
            return;
          }
        }
        self.emit(type, data, socket);
      });
    });

    // Handle errors by tracking them.
    socket.on('error', function (error) {
      self.error(error);
    });

    // Send data if the socket is writable, otherwise queue it.
    socket.send = function (type, data) {
      if (socket.writable) {
        socket.write(type + '~' + (JSON.stringify(data) || null) + '\n');
      }
      else {
        socket.queue.push(arguments);
      }
    };
  },

  /**
   * When errors occur, track them, and mark the peer as down.
   */
  error: function (error) {
    var self = this;
    var client = self.client;

    // Add the error to a list of recent errors.
    error.message += ' (' + client.name + ' -> ' + self.name + ')';
    var code = error.code;
    var errors = self.errors;
    errors[code] = errors[code] || new Errors();
    errors[code].add(error);

    // If this is a peer, mark it as down.
    if (self != client) {
      self.status = Peer.DOWN;
    }

    // Try to reconnect.
    setTimeout(function () {
      self.connect();
    }, error.code == 'ENOTFOUND' ? 1e5 : 1e2);
  },

  /**
   * Send a message over the socket.
   */
  send: function (type, data) {
    var self = this;
    self.socket.send.apply(self, arguments);
  },

  /**
   * Get a key value from a peer, and run a callback.
   */
  getValue: function (key, fn) {
    var self = this;
    self.once('get:' + key, fn);
    self.socket.send('get', key);
  },

  /**
   * Set a value for a key on a peer, and run a callback.
   */
  setValue: function (key, value, fn) {
    var self = this;
    self.once('set:' + key, fn);
    self.socket.send('set', {key: key, value: value});
  }

});

Peer.UNKNOWN = 0;
Peer.UP = 1;
Peer.DOWN = 2;
