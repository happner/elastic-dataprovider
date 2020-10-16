module.exports = {
  get: function(dbPath, testId) {
    var happnConfig = require('./happn-config').get(dbPath, testId);

    return {
      happn: happnConfig
    };
  }
};
