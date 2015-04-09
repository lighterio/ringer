

  /**
   * Set HTTP routes for showing ring client information.
   */
  setApp: function (app) {
    var self = this;

    app.get('/ringer', function (request, response) {
      response.send(self);
    });

    app.get('/ringer/peers', function (request, response) {
      response.send(self.peers);
    });

    app.get('/ringer/roster', function (request, response) {
      response.send({
        client: self.name,
        roster: self.getRoster(),
      });
    });

    app.get('/ringer/frames', function (request, response) {
      response.setHeader('content-type', 'text/html');
      var html = '<title>Ringer</title>';
      for (var i = 0; i < self.processCount; i++) {
        var port = app.httpPort + i;
        html += '<iframe src="http://localhost:' + port + '/ringer/roster" style="width:100%"></iframe><br>';
      }
      response.send(html);
    });

  }
