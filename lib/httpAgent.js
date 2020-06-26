var elasticsearch = require('elasticsearch');

var util = require('util');

var HttpConnector = require('elasticsearch/src/lib/connectors/http');

var customHttpAgent = require('agentkeepalive');

function CustomESHTTPConnector(host, config) {
  HttpConnector.call(this, host, config);
}

util.inherits(CustomESHTTPConnector, HttpConnector);

CustomESHTTPConnector.prototype.createAgent = function(config) {
  return new customHttpAgent(this.makeAgentConfig(config));
};

module.exports = CustomESHTTPConnector;
