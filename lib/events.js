var Type = require('../common/object/type');

/**
 * Events are bound to a peer, which is an event emitter.
 */
var Events = module.exports = Type.extend({

  /**
   * Once a socket has connected, send any queued messages.
   */
  'socket:connected': function (socket) {
    var self = this;
    socket.queue.forEach(function (args) {
      var type = args[0];
      var data = args[1];
      self.sent++;
      socket.write(type + '~' + JSON.stringify(data) + '\n');
    });
  },

  /**
   * Sync a peer's roster with the client's roster.
   */
  'roster:sync': function (data, socket) {
    var self = this;
    var client = self.client;
    socket.name = data.name;

    // Each roster represents the peers an external client knows.
    var peerRoster = data.roster;
    var roster = client.getRoster();
    var isMatch = (JSON.stringify(peerRoster) == JSON.stringify(roster));
    var response = {name: client.name, isMatch: isMatch, add: {}};

    // If the rosters don't match, add the peer's peers to the client.
    if (!isMatch) {
      for (var name in peerRoster) {
        if (!client.peers[name]) {
          client.addPeer(name);
          response.add[name] = Peer.UNKNOWN;
        }
      }
    }

    // Respond with any peers that should be added to the peer.
    socket.send('roster:add', response);
  },

  /**
   * Add peers from a peer's roster.
   */
  'roster:add': function (data, socket) {
    var self = this;
    var client = self.client;
    socket.name = data.name;

    // Add hosts that weren't known.
    for (var name in data.add) {
      client.addPeer(name);
    }

    // If the rosters matched, we can stabilize.
    if (data.isMatch) {
      var discover = client.discover;
      discover.matchCount++;
      if (discover.matchCount == discover.requestCount) {
        if (!client.isStable) {
          client.stabilize();
        }
      }
    }
  },

  /**
   * Respond to a heartbeat by relaying the data back.
   */
  'heartbeat:start': function (data, socket) {
    socket.send('heartbeat:end', data);
  },

  /**
   * When a heartbeat finishes its roundtrip, update latency.
   */
  'heartbeat:end': function (data, socket) {
    var self = this;
    var client = self.client;
    var elapsed = (Date.now() - data.start) * 1e3;
    var divisor = client.decayDivisor;
    var peer = client.peers[socket.name];
    if (peer) {
      peer.latency += Math.round((elapsed - peer.latency) / divisor);
    }
  },

  /**
   * Get a value from a key.
   */
  get: function (key, socket) {
    var self = this;
    self.getValue(key, function (value) {
      socket.send('get:' + key, value);
    });
  },

  /**
   * Set a value for a key.
   */
  set: function (item, socket) {
    var self = this;
    self.setValue(item.key, item.value, function () {
      socket.send('set:' + item.key);
    });
  },

  /**
   * Get a block of IDs for a specified sequence.
   */
  'sequence:getBlock': function (name, socket) {
    var self = this;
    var key = name + '.sequence';
    self.getValue(key, function () {

      // If there's existing sequence information, allocate a block.
      if (value) {
        var first = value.next;
        var last = first + SEQUENCE_BLOCK_SIZE - 1;
        var next = last + 1;
        var response = {next: next, block: {next: first, last: last}};
        value.next = next;
        self.setValue(key, value, function () {
          socket.send('sequence:block', value);
        });
      }

      //
      else {
        self.setValue(key, {next: 1, blocks: []}, function () {
          self.emit('sequence:get', name, socket);
        });
      }
    });
  }

});

var SEQUENCE_BLOCK_SIZE = 1e3;
