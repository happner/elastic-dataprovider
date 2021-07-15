describe('func-indexes, with route and data caching', function() {
  this.timeout(5000);
  const test = require('./fixtures/test-helper');
  var expect = require('expect.js');

  var service = require('..');

  var testId = require('uuid')
    .v4()
    .split('-')
    .join('');

  var path = require('path');

  var provider_path = path.resolve('../index.js');

  var async = require('async');

  var random = require('./fixtures/random');

  var uuid = require('uuid');

  var config = {
    name: 'elastic',
    provider: provider_path,
    defaultIndex: 'indextest',
    host: test.getEndpoint(),
    cache: true,
    routeCache: true,
    indexes: [
      {
        index: 'indextest',
        body: {
          mappings: {}
        }
      },
      {
        index: 'custom'
      }
    ],
    dataroutes: [
      {
        pattern: '/custom/*',
        index: 'custom'
      },
      {
        dynamic: true, //dynamic routes generate a new index/type according to the items in the path
        pattern: '/dynamic/{{index}}/{{type}}/*'
      },
      {
        pattern: '*',
        index: 'indextest'
      }
    ]
  };

  var serviceInstance = new service(config);

  before('should initialize the service', function(callback) {
    if (!serviceInstance.happn)
      serviceInstance.happn = {
        services: {
          utils: {
            wildcardMatch: function(pattern, matchTo) {
              var regex = new RegExp(pattern.replace(/[*]/g, '.*'));
              var matchResult = matchTo.match(regex);

              if (matchResult) return true;

              return false;
            }
          }
        }
      };

    serviceInstance.initialize(callback);
  });

  after(function(done) {
    serviceInstance.stop(done);
  });

  var getElasticClient = function(callback) {
    var elasticsearch = require('elasticsearch');

    try {
      var client = new elasticsearch.Client({ host: 'localhost:9200' });

      client.ping(
        {
          requestTimeout: 30000
        },
        function(e) {
          if (e) return callback(e);

          callback(null, client);
        }
      );
    } catch (e) {
      callback(e);
    }
  };

  var listAll = function(client, index, type, callback) {
    var elasticMessage = {
      index: index,
      type: type,
      body: {
        from: 0,
        size: 10000
      }
    };

    client
      .search(elasticMessage)

      .then(function(resp) {
        if (resp.hits && resp.hits.hits && resp.hits.hits.length > 0) {
          callback(null, resp.hits.hits);
        } else callback(null, []);
      })
      .catch(function(e) {
        callback(e);
      });
  };

  it('sets data with custom path, and data with default path, we then query the data directly and ensure our counts are right', function(done) {
    serviceInstance.upsert('/custom/' + testId, { data: { test: 'custom' } }, {}, false, function(
      e,
      response,
      created
    ) {
      if (e) return done(e);

      serviceInstance.upsert(
        '/default/' + testId,
        { data: { test: 'default' } },
        {},
        false,
        function(e, response, created) {
          if (e) return done(e);

          getElasticClient(function(e, client) {
            if (e) return done(e);

            var foundItems = [];

            setTimeout(function() {
              listAll(client, 'indextest', 'happner', function(e, defaultItems) {
                if (e) return done(e);

                listAll(client, 'custom', 'happner', function(e, customItems) {
                  if (e) return done(e);

                  defaultItems.forEach(function(item) {
                    if (item._id == '/default/' + testId) foundItems.push(item);
                  });

                  expect(foundItems.length).to.be(1);

                  foundItems = [];

                  customItems.forEach(function(item) {
                    if (item._id == '/custom/' + testId) foundItems.push(item);
                  });

                  expect(foundItems.length).to.be(1);

                  done();
                });
              });
            }, 1000);
          });
        }
      );
    });
  });

  it('tests dynamic routes', function(done) {
    var now1 = Date.now().toString();

    var now2 = Date.now().toString();

    var path1 = '/dynamic/' + testId + '/dynamicType0/dynamicValue0/' + now1;

    var path2 = '/dynamic/' + testId + '/dynamicType1/dynamicValue0/' + now2;

    serviceInstance.upsert(path1, { data: { test: 'dynamic0' } }, {}, false, function(
      e,
      response,
      created
    ) {
      if (e) return done(e);

      serviceInstance.upsert(path2, { data: { test: 'dynamic1' } }, {}, false, function(
        e,
        response,
        created
      ) {
        if (e) return done(e);

        getElasticClient(function(e, client) {
          if (e) return done(e);

          setTimeout(function() {
            listAll(client, testId, 'dynamicType0', function(e, dynamictems0) {
              if (e) return done(e);

              listAll(client, testId, 'dynamicType1', function(e, dynamictems1) {
                if (e) return done(e);

                expect(dynamictems0.length).to.be(1);

                expect(dynamictems0[0]._index).to.be(testId);

                expect(dynamictems0[0]._type).to.be('dynamicType0');

                expect(dynamictems0[0]._source.path).to.be(path1);

                expect(dynamictems1.length).to.be(1);

                expect(dynamictems1[0]._index).to.be(testId);

                expect(dynamictems1[0]._type).to.be('dynamicType1');

                expect(dynamictems1[0]._source.path).to.be(path2);

                done();
              });
            });
          }, 1000);
        });
      });
    });
  });

  var ROUTE_COUNT = 20;
  var ROW_COUNT = 100;
  var DELAY = 500;

  it(
    'tests parallel dynamic routes, creating ' +
      ROUTE_COUNT +
      ' routes and pushing ' +
      ROW_COUNT +
      ' data items into the routes',
    function(done) {
      this.timeout(1000 * ROW_COUNT + DELAY);

      var routes = [];
      var rows = [];
      var errors = [];
      var successes = [];

      for (var i = 0; i < ROUTE_COUNT; i++) {
        var index = (uuid.v4() + uuid.v4()).toLowerCase().replace(/\-/g, '');
        var route = '/dynamic/' + index + '/test_type';
        routes.push(route);
      }

      for (var i = 0; i < ROW_COUNT; i++) {
        var routeIndex = random.integer(0, ROUTE_COUNT);

        if (routes[routeIndex] != null)
          rows.push(
            routes[routeIndex] +
              '/route_' +
              routeIndex.toString() +
              '/' +
              Date.now().toString() +
              '/' +
              routeIndex.toString()
          );
      }

      async.each(
        rows,
        function(row, callback) {
          serviceInstance.upsert(row, { data: { test: row } }, {}, false, function(
            e,
            response,
            created
          ) {
            if (e) {
              errors.push({ row: row, error: e });
              return callback(e);
            }

            successes.push({ row: row, created: created });

            callback();
          });
        },
        function(e) {
          if (e) return done(e);

          var errorHappened = false;

          setTimeout(function() {
            async.each(
              successes,
              function(successfulRow, successfulRowCallback) {
                var callbackError = function(error) {
                  if (!errorHappened) {
                    errorHappened = true;
                    successfulRowCallback(error);
                  }
                };

                serviceInstance.find(successfulRow.row, {}, function(e, data) {
                  if (e) return callbackError(e);

                  if (data.length == 0)
                    return callbackError(new Error('missing row for: ' + successfulRow.row));

                  if (data[0].data.test != successfulRow.row)
                    return callbackError(
                      new Error(
                        'row test value ' +
                          data[0].data.test +
                          ' was not equal to ' +
                          successfulRow.row
                      )
                    );

                  successfulRowCallback();
                });
              },
              done
            );
          }, DELAY);
        }
      );
    }
  );
});
