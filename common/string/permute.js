/**
 * Return permutations of a string using 2 types of parentheticals.
 *  - The string "(a|b|c)" would return ["a", "b", "c"].
 *  - The string "(1-3)" would return ["1", "2", "3"].
 *
 * @origin https://github.com/lighterio/lighter-common/common/string/permute.js
 * @version 0.0.1
 */

var permute = module.exports = function (input) {
  var permutations;
  input = '' + input;
  input.replace(/^(.*)\(([^\)]+)\)(.*)$/, function (match, start, spec, end) {
    var variations;
    spec.replace(/([0-9]+)\-([0-9]+)/, function (match, first, last) {
      variations = [];
      first *= 1;
      last *= 1;
      for (var n = first; n <= last; n++) {
        variations.push(n);
      }
    });
    variations = variations || spec.split('|');

    permutations = [];
    variations.forEach(function (variation) {
      permute(start + variation + end).forEach(function (permutation) {
        permutations.push(permutation);
      });
    });
  });
  return permutations || [input];
};