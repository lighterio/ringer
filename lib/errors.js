var Type = require('../common/object/type');

/**
 * Track a list of recent errors with a total and a running frequency.
 */
module.exports = Type.extend({

  // Number of recent errors to remember.
  recentLimit: 10,

  // A larger divisor makes for a slower-changing running mean.
  decayDivisor: 10,

  // Initialize a new set of errors.
  init: function (options) {
    var self = this;

    Type.decorate(self, options);

    // Total number of errors.
    self.count = 0;

    // Last time (in epoch milliseconds) that an error occurred.
    self.lastTime = 0;

    // Running mean of microseconds between errors.
    self.interval = 0;

    // Array of recent errors.
    self.recent = [];

    // Errors per second.
    Object.defineProperty(self, 'frequency', {
      get: function () {
        return 1e6 / self.interval;
      }
    });
  },

  // Add a new error to the list.
  add: function (error) {
    var self = this;

    // Store the time of this error on the error object.
    var now = error.time = Date.now();

    // If at the limit, pop an old error out before adding the new one.
    if (self.count >= self.recentLimit) {
      self.recent.pop();
    }
    self.recent.unshift(error);

    // If we've seen an error before, we can calculate the interval.
    if (self.lastTime) {

      // Calculate true mean until the count is greater than the decay divisor.
      var divisor = Math.min(self.count, self.decayDivisor);

      // Convert the elapsed time to microseconds.
      var elapsed = (now - self.lastTime) * 1e3;

      // Decay part of the old value and add the new value.
      self.interval += Math.round((elapsed - self.interval) / divisor);
    }

    // Increment the number of errors seen.
    self.count++;

    // Record the current time as the time of the last error.
    self.lastTime = now;
  }

});
