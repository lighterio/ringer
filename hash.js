var values = 1e6;
var peers = 1e3;
var divisor = Math.pow(2, 32) / peers;

var crc32 = require('./common/crypto/crc32');
var crc32b = require('./common/crypto/crc32b');
var xxhash = require('xxhash-nan').hash;

var shards = new Array(peers);
var keys = new Array(values);

var tests = {

  xxhash: function () {
    var k, b, i, p;
    for (i = 0; i < values; i++) {
      k = keys[i];
      b = new Buffer(k);
      p = Math.floor(xxhash(b, 0xCAFEBABE) / divisor);
      shards[p]++;
    }
  },

  crc32: function () {
    var k, i, p;
    for (i = 0; i < values; i++) {
      k = keys[i];
      p = Math.floor(crc32(k) / divisor);
      shards[p]++;
    }
  },

  crc32b: function () {
    var k, i, p;
    for (i = 0; i < values; i++) {
      k = keys[i];
      p = Math.floor(crc32b(k) / divisor);
      shards[p]++;
    }
  }

};

var generators = {

  Advertiser: function (i) {
    return 'e:a=' + (i + 1);
  },

  "Site + User": function (i) {
    return 'e:s,u=' + (i % 1e2 + 1) + ',' + (i + 1);
  },

  Timestamp: function (i) {
    return (new Date(1421e9 + i * 1e7)).toISOString();
  },

  ProcessJson: function (i) {
    return JSON.stringify({
      advertiser: this.Advertiser(i),
      siteAndUser: this['Site + User'](i),
      timestamp: this.Timestamp(i)
    });
  }

};

console.log();
for (var scheme in generators) {

  console.log(scheme + ' keys:');

  var generator = generators[scheme];
  for (var i = 0; i < values; i++) {
    keys[i] = generator.call(generators, i);
  }

  for (var n in tests) {
    for (i = 0; i < peers; i++) {
      shards[i] = 0;
    }

    var t = Date.now();
    tests[n]();
    t = Date.now() - t;

    var e = 0;
    shards.forEach(function (shard) {
      e += Math.abs(shard - values / peers);
    });

    var p = Math.round(e / values * 1000) / 10;
    console.log('  ' + n + ': ' + t + ' ms, ' + p + '% imbalance');
  }
  console.log();
  console.log(keys[i - 1]);
  console.log();
}

