var net = require('net');
var log;

module.exports = Peer;

function Peer(name, ring) {
  log = ring.logger;
  var peer = this;
  var pair = name.split(':');
  peer.name = name;
  peer.host = pair[0];
  peer.port = pair[1];
  peer.connect();
}

Peer.prototype.connect = function () {
  var peer = this;
  peer.socket = net.connect(peer.port, peer.host, function (error) {
    if (error) {
      log.error('[Ringer] Failed to connect to "' + peer.name + '"', error);
    }
    peer.write('HELLO!!!');
  });
};

Peer.prototype.write = function (type, message) {
  peer.socket.write(type + ' ' + JSON.stringify(message));
};