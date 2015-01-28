var values = 1e6;
var peers = 1e3;
var divisor = Math.pow(2, 32) / peers;

var crcjs = require('/Users/sam/Workspace/lighter-common/common/crypto/crc32');
var xxhash = require('xxhash-nan').hash;

var shards = new Array(peers);
var keys = new Array(values);

var tests = {

  crcjs: function () {
    var k, i, p;
    for (i = 0; i < values; i++) {
      k = keys[i];
      p = Math.floor(crcjs(k) / divisor);
      shards[p]++;
      //console.log(k + ': ' + p);
    }
  },

  xxhash: function () {
    var k, b, i, p;
    for (var i = 0; i < values; i++) {
      k = keys[i];
      b = new Buffer(k);
      p = Math.floor(xxhash(b, 0xCAFEBABE) / divisor);
      shards[p]++;
      //console.log(k + ': ' + p);
    }
  }

};

var generators = {

  Advertiser: function (i) {
    return 'Advertiser' + (i + 1);
  },

  "Site + User": function (i) {
    return 'Site' + (i % 1e2 + 1) + ',User' + (i + 1);
  },

  Timestamp: function (i) {
    return (new Date(1421e9 + i * 1e7)).toISOString();
  }

};

console.log();
for (var scheme in generators) {

  console.log(scheme + ' keys:');

  var generator = generators[scheme];
  for (var i = 0; i < values; i++) {
    keys[i] = generator(i);
  }

  for (var name in tests) {
    for (i = 0; i < peers; i++) {
      shards[i] = 0;
    }

    var t = Date.now();
    tests[name]();
    t = Date.now() - t;

    var e = 0;
    shards.forEach(function (shard) {
      e += Math.abs(shard - values / peers);
    });

    console.log('  ' + name + ': ' + t + ' milliseconds, ' + e + ' imbalance');
  }
  console.log();
}
