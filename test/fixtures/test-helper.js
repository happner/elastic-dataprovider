require('custom-env').env('test');
module.exports = class TestHelper {
  static getEndpoint() {
    return process.env.ENDPOINT || 'http://localhost:9200';
  }
};
