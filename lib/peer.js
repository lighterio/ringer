var log;

module.exports = Peer;

function Peer(name, ring) {
  log = ring.logger;
  var peer = this;
  var pair = name.split(':');
  peer.name = name;
  peer.host = pair[0];
  peer.port = pair[1];
}

Peer.prototype.connect = function () {

};
