describe('perf', function() {
  this.timeout(5000);
  const test = require('./fixtures/test-helper');
  var methodAnalyzer = require('happner-profane').create();

  var expect = require('expect.js');

  var service = methodAnalyzer.require('../index.js', true);

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
    methodAnalyzer.cleanup();

    var upsertAnalytics = methodAnalyzer.getAnalysis();

    // eslint-disable-next-line no-console
    console.log('method analysis:::', JSON.stringify(upsertAnalytics, null, 2));

    serviceInstance.stop(done);
  });

  var ROUTE_COUNT = 5;

  var ROW_COUNT = 1000;

  var DELAY = 2000;

  it(
    'tests parallel dynamic routes, creating ' +
      ROUTE_COUNT +
      ' routes and pushing ' +
      ROW_COUNT +
      ' data items into the routes via the upsert operation',
    function(done) {
      this.timeout(1000 * ROW_COUNT + DELAY);

      var routes = [];
      var rows = [];
      var errors = [];
      var successes = [];

      for (var i = 0; i < ROUTE_COUNT; i++) {
        var index = (uuid.v4() + uuid.v4()).toLowerCase().replace(/\-/g, '');
        var route = '/dynamic/' + index + '/test_type_upsert';
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
              '/upsert/' +
              routeIndex.toString()
          );
      }

      var started = Date.now();

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

          var duration = Date.now() - started;

          console.log('duration of push: ', duration);

          var rate = 1000 / (duration / ROW_COUNT);

          console.log(
            'upserted at a rate of ' +
              rate +
              ' per sec, delay of ' +
              DELAY +
              'ms before confirming via find...'
          );

          setTimeout(function() {
            async.eachSeries(
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
              function(e) {
                if (e) return done(e);

                console.log('data confirmed in database.');

                done();
              }
            );
          }, DELAY);
        }
      );
    }
  );

  it(
    'tests parallel dynamic routes, creating ' +
      ROUTE_COUNT +
      ' routes and pushing ' +
      ROW_COUNT +
      ' data items into the routes via the upsert operation with refresh = false',
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
              '/upsert/norefresh/' +
              routeIndex.toString()
          );
      }

      var started = Date.now();

      async.each(
        rows,
        function(row, callback) {
          serviceInstance.upsert(row, { data: { test: row } }, { refresh: false }, false, function(
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

          var duration = Date.now() - started;

          console.log('duration of push: ', duration);

          var rate = 1000 / (duration / ROW_COUNT);

          console.log(
            'upserted at a rate of ' +
              rate +
              ' per sec, delay of ' +
              DELAY +
              'ms before confirming via find...'
          );

          setTimeout(function() {
            async.eachSeries(
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
              function(e) {
                if (e) return done(e);

                console.log('data confirmed in database.');

                done();
              }
            );
          }, DELAY);
        }
      );
    }
  );

  var ROUTE_COUNT_INSERT = 5;
  var ROW_COUNT_INSERT = 1000;
  var DELAY_INSERT = 2000;

  it(
    'tests parallel dynamic routes, creating ' +
      ROUTE_COUNT_INSERT +
      ' routes and pushing ' +
      ROW_COUNT_INSERT +
      ' data items into the routes via the insert operation',
    function(done) {
      this.timeout(1000 * ROW_COUNT_INSERT + DELAY_INSERT);

      var routes = [];
      var rows = [];
      var errors = [];
      var successes = [];

      for (var i = 0; i < ROUTE_COUNT_INSERT; i++) {
        var index = (uuid.v4() + uuid.v4()).toLowerCase().replace(/\-/g, '');
        var route = '/dynamic/' + index + '/test_type';
        routes.push(route);
      }

      for (var i = 0; i < ROW_COUNT_INSERT; i++) {
        var routeIndex = random.integer(0, ROUTE_COUNT_INSERT);

        if (routes[routeIndex] != null)
          rows.push(
            routes[routeIndex] +
              '/route_' +
              routeIndex.toString() +
              '/' +
              Date.now().toString() +
              '/' +
              i.toString()
          );
      }

      var started = Date.now();

      async.each(
        rows,
        function(row, callback) {
          serviceInstance.upsert(
            row,
            { data: { test: row } },
            { upsertType: serviceInstance.UPSERT_TYPE.insert },
            false,
            function(e, response, created) {
              if (e) {
                errors.push({ row: row, error: e });
                return callback(e);
              }

              successes.push({ row: row, created: created });

              callback();
            }
          );
        },
        function(e) {
          if (e) return done(e);

          var errorHappened = false;

          var duration = Date.now() - started;

          console.log('duration of push: ', duration);

          var rate = 1000 / (duration / ROW_COUNT_INSERT);

          console.log(
            'inserted at a rate of ' +
              rate +
              ' per sec, DELAY_INSERT of ' +
              DELAY_INSERT +
              'ms before confirming via find...'
          );

          setTimeout(function() {
            async.eachSeries(
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
              function(e) {
                if (e) return done(e);

                console.log('data confirmed in database.');

                done();
              }
            );
          }, DELAY_INSERT);
        }
      );
    }
  );

  it(
    'tests parallel dynamic routes, creating ' +
      ROUTE_COUNT_INSERT +
      ' routes and pushing ' +
      ROW_COUNT_INSERT +
      ' data items into the routes via the insert operation, refresh = false',
    function(done) {
      this.timeout(1000 * ROW_COUNT_INSERT + DELAY_INSERT);

      var routes = [];
      var rows = [];
      var errors = [];
      var successes = [];

      for (var i = 0; i < ROUTE_COUNT_INSERT; i++) {
        var index = (uuid.v4() + uuid.v4()).toLowerCase().replace(/\-/g, '');

        var route = '/dynamic/' + index + '/test_type';

        routes.push(route);
      }

      for (var i = 0; i < ROW_COUNT_INSERT; i++) {
        var routeIndex = random.integer(0, ROUTE_COUNT_INSERT);

        if (routes[routeIndex] != null)
          rows.push(
            routes[routeIndex] +
              '/route_' +
              routeIndex.toString() +
              '/' +
              Date.now().toString() +
              '/norefresh/' +
              '/' +
              i.toString()
          );
      }

      var started = Date.now();

      async.each(
        rows,
        function(row, callback) {
          serviceInstance.upsert(
            row,
            { data: { test: row } },
            { upsertType: serviceInstance.UPSERT_TYPE.insert, refresh: false },
            false,
            function(e, response, created) {
              if (e) {
                errors.push({ row: row, error: e });
                return callback(e);
              }

              successes.push({ row: row, created: created });

              callback();
            }
          );
        },
        function(e) {
          if (e) return done(e);

          var errorHappened = false;

          var duration = Date.now() - started;

          console.log('duration of push: ', duration);

          var rate = 1000 / (duration / ROW_COUNT_INSERT);

          console.log(
            'inserted at a rate of ' +
              rate +
              ' per sec, DELAY_INSERT of ' +
              DELAY_INSERT +
              'ms before confirming via find...'
          );

          setTimeout(function() {
            async.eachSeries(
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
              function(e) {
                if (e) return done(e);

                console.log('data confirmed in database.');

                done();
              }
            );
          }, DELAY_INSERT);
        }
      );
    }
  );

  it(
    'tests parallel non-dynamic routes, creating ' +
      ROUTE_COUNT_INSERT +
      ' routes and pushing ' +
      ROW_COUNT_INSERT +
      ' data items into the routes via the insert operation, refresh = false',
    function(done) {
      this.timeout(1000 * ROW_COUNT_INSERT + DELAY_INSERT);

      var routes = [];
      var rows = [];
      var errors = [];
      var successes = [];

      for (var i = 0; i < ROUTE_COUNT_INSERT; i++) {
        var index = (uuid.v4() + uuid.v4()).toLowerCase().replace(/\-/g, '');

        var route = '/non_dynamic/' + index + '/test_type';

        routes.push(route);
      }

      for (var i = 0; i < ROW_COUNT_INSERT; i++) {
        var routeIndex = random.integer(0, ROUTE_COUNT_INSERT);

        if (routes[routeIndex] != null)
          rows.push(
            routes[routeIndex] +
              '/route_' +
              routeIndex.toString() +
              '/' +
              Date.now().toString() +
              '/norefresh/' +
              '/' +
              i.toString()
          );
      }

      var started = Date.now();

      async.eachSeries(
        rows,
        function(row, callback) {
          serviceInstance.upsert(
            row,
            { data: { test: row } },
            { upsertType: serviceInstance.UPSERT_TYPE.insert, refresh: false },
            false,
            function(e, response, created) {
              if (e) {
                errors.push({ row: row, error: e });
                return callback(e);
              }

              successes.push({ row: row, created: created });

              callback();
            }
          );
        },
        function(e) {
          if (e) return done(e);

          var errorHappened = false;

          var duration = Date.now() - started;

          console.log('duration of push: ', duration);

          var rate = 1000 / (duration / ROW_COUNT_INSERT);

          console.log(
            'inserted at a rate of ' +
              rate +
              ' per sec, DELAY_INSERT of ' +
              DELAY_INSERT +
              'ms before confirming via find...'
          );

          setTimeout(function() {
            async.eachSeries(
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
              function(e) {
                if (e) return done(e);

                console.log('data confirmed in database.');

                done();
              }
            );
          }, DELAY_INSERT);
        }
      );
    }
  );

  var ROW_COUNT_DIRECT = 1000;
  var DELAY_DIRECT = 2000;

  xit('tests direct pushes to ES, pushing ' + ROW_COUNT_DIRECT + ' data items', function(done) {
    this.timeout(1000 * ROW_COUNT_DIRECT + DELAY_DIRECT);

    var routes = [];
    var rows = [];
    var errors = [];
    var successes = [];

    for (var i = 0; i < 1; i++) {
      var index = (uuid.v4() + uuid.v4()).toLowerCase().replace(/\-/g, '');
      var route = '/dynamic/' + index + '/test_type';
      routes.push(route);
    }

    for (var i = 0; i < ROW_COUNT_DIRECT; i++) {
      rows.push(routes[0] + '/route_1/' + Date.now().toString() + '/' + i.toString());
    }

    var started = Date.now();

    getElasticClient(function(e, client) {
      if (e) return done(e);

      async.each(
        rows,
        function(row, callback) {
          var elasticMessage = {
            index: 'happn',
            type: 'happn',
            id: row,
            body: { data: { test: row } },
            refresh: false
          };

          client.create(elasticMessage, function(e, created) {
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

          var duration = Date.now() - started;

          console.log('duration of push: ', duration);

          var rate = 1000 / (duration / ROW_COUNT_DIRECT);

          console.log(
            'DIRECTed at a rate of ' +
              rate +
              ' per sec, DELAY_DIRECT of ' +
              DELAY_DIRECT +
              'ms before confirming via find...'
          );

          setTimeout(function() {
            async.eachSeries(
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
              function(e) {
                if (e) return done(e);

                console.log('data confirmed in database.');

                done();
              }
            );
          }, DELAY_DIRECT);
        }
      );
    });
  });

  xit(
    'tests direct pushes to ES, pushing ' + ROW_COUNT_DIRECT + ' data items with http agent',
    function(done) {
      this.timeout(1000 * ROW_COUNT_DIRECT + DELAY_DIRECT);

      var routes = [];
      var rows = [];
      var errors = [];
      var successes = [];

      for (var i = 0; i < 1; i++) {
        var index = (uuid.v4() + uuid.v4()).toLowerCase().replace(/\-/g, '');
        var route = '/dynamic/' + index + '/test_type';
        routes.push(route);
      }

      for (var i = 0; i < ROW_COUNT_DIRECT; i++) {
        rows.push(routes[0] + '/route_1/' + Date.now().toString() + '/' + i.toString());
      }

      var started = Date.now();

      getElasticClientAgent(function(e, client) {
        if (e) return done(e);

        async.each(
          rows,
          function(row, callback) {
            var elasticMessage = {
              index: 'happn',
              type: 'happn',
              id: row,
              body: { data: { test: row } },
              refresh: false
            };

            client.create(elasticMessage, function(e, created) {
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

            var duration = Date.now() - started;

            console.log('duration of push: ', duration);

            var rate = 1000 / (duration / ROW_COUNT_DIRECT);

            console.log(
              'DIRECTed at a rate of ' +
                rate +
                ' per sec, DELAY_DIRECT of ' +
                DELAY_DIRECT +
                'ms before confirming via find...'
            );

            setTimeout(function() {
              async.eachSeries(
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
                function(e) {
                  if (e) return done(e);

                  console.log('data confirmed in database.');

                  done();
                }
              );
            }, DELAY_DIRECT);
          }
        );
      });
    }
  );

  var BULK_ROUTE_COUNT = 5;

  var BULK_ROW_COUNT = 1000;

  var BULK_INSERT_COUNT = 0;

  var BULK_DELAY = 5000;

  it(
    'tests parallel dynamic routes, creating ' +
      BULK_ROUTE_COUNT +
      ' routes and pushing ' +
      BULK_ROW_COUNT +
      ' data items into the routes via the bulk operation',
    function(done) {
      this.timeout(1000 * BULK_ROW_COUNT + BULK_DELAY);

      var routes = [];

      var bulkData = [];

      for (var i = 0; i < BULK_ROUTE_COUNT; i++) {
        var index = (uuid.v4() + uuid.v4()).toLowerCase().replace(/\-/g, '');

        var type = (uuid.v4() + uuid.v4()).toLowerCase().replace(/\-/g, '');

        routes.push({ index: index, type: type });
      }

      for (var i = 0; i < BULK_ROW_COUNT; i++) {
        var routeIndex = random.integer(0, BULK_ROUTE_COUNT - 1);

        bulkData.push({
          data: { index: routes[routeIndex].index, type: routes[routeIndex].type }
        });

        BULK_INSERT_COUNT++;
      }

      var started = Date.now();

      serviceInstance.upsert(
        '/dynamic/{{index}}/{{type}}/{id}/bulk_dynamic_' + testId,
        bulkData,
        { upsertType: 3 },
        false,
        function(e, inserted) {
          if (e) return done(e);

          expect(inserted.errors).to.be(false);

          expect(inserted.items.length).to.be(BULK_INSERT_COUNT);

          var duration = Date.now() - started;

          console.log('duration of push: ', duration);

          var rate = 1000 / (duration / BULK_INSERT_COUNT);

          console.log(
            'upserted at a rate of ' +
              rate +
              ' per sec, BULK_DELAY of ' +
              BULK_DELAY +
              'ms before confirming via find...'
          );

          setTimeout(function() {
            async.eachSeries(
              inserted.items,
              function(item, itemCB) {
                serviceInstance.find(item.index._id, {}, function(e, data) {
                  if (e) return itemCB(e);

                  if (data.length == 0)
                    return done(new Error('missing row for: ' + item.index._id));

                  itemCB();
                });
              },
              done
            );
          }, BULK_DELAY);
        }
      );
    }
  );

  it(
    'tests parallel static routes (index and type is constant) pushing ' +
      BULK_ROW_COUNT +
      ' data items into the routes via the bulk operation',
    function(done) {
      this.timeout(1000 * BULK_ROW_COUNT + BULK_DELAY);

      BULK_INSERT_COUNT = 0;

      var bulkData = [];

      for (var i = 0; i < BULK_ROW_COUNT; i++) {
        bulkData.push({
          data: { index: 'bulk', type: 'bulk' }
        });

        BULK_INSERT_COUNT++;
      }

      var started = Date.now();

      serviceInstance.upsert(
        '/dynamic/{{index}}/{{type}}/bulk_static' + testId + '/{id}',
        bulkData,
        { upsertType: 3 },
        false,
        function(e, inserted) {
          if (e) return done(e);

          expect(inserted.errors).to.be(false);

          expect(inserted.items.length).to.be(BULK_INSERT_COUNT);

          //console.log(JSON.stringify(inserted.items, null, 2));

          var duration = Date.now() - started;

          console.log('duration of push: ', duration);

          var rate = 1000 / (duration / BULK_INSERT_COUNT);

          console.log(
            'upserted at a rate of ' +
              rate +
              ' per sec, BULK_DELAY of ' +
              BULK_DELAY +
              'ms before confirming via find...'
          );

          setTimeout(function() {
            // /dynamic/happn/happn/bulk_static4d55362bc5944eb7877b332c4acab7b1/Y3sY4o9iRwyK3RUZDK7fjA-9

            serviceInstance.find('/dynamic/bulk/bulk/bulk_static' + testId + '/*', {}, function(
              e,
              data
            ) {
              if (e) return done(e);

              if (data.length != BULK_INSERT_COUNT)
                return done(
                  new Error('missing rows for: ' + '/dynamic/bulk/bulk/bulk_static' + testId + '*')
                );

              done();
            });
          }, BULK_DELAY);
        }
      );
    }
  );

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

  var getElasticClientAgent = function(callback) {
    var AgentKeepAlive = require('agentkeepalive');

    var elasticsearch = require('elasticsearch');

    var baseConfig = {};

    baseConfig.createNodeAgent = function(connection, config) {
      return new AgentKeepAlive(connection.makeAgentConfig(config));
    };

    try {
      var client = new elasticsearch.Client(baseConfig);

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
        sort: [{ timestamp: { order: 'asc' } }],
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
});
