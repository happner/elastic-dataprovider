describe('sanity-happn', function() {
  var expect = require('expect.js');
  var happn = require('happn-3');
  var service = happn.service;
  var async = require('async');
  var happnInstance = null;
  const delay = require('await-delay');

  var test_id = Date.now() + '_' + require('shortid').generate();

  var path = require('path');

  this.timeout(60000);

  var db_path = path.resolve(__dirname.replace('test', '')) + path.sep + 'index.js';

  var config = require('./fixtures/happn-config').get(db_path, test_id);

  before('should initialize the service', function(callback) {
    try {
      service.create(
        config,

        function(e, happnInst) {
          if (e) return callback(e);

          happnInstance = happnInst;

          callback();
        }
      );
    } catch (e) {
      callback(e);
    }
  });

  after(function(done) {
    happnInstance.stop(done);
  });

  var publisherclient;
  var listenerclient;

  /*
   We are initializing 2 clients to test saving data against the database, one client will push data into the
   database whilst another listens for changes.
   */
  before('should initialize the clients', function(callback) {
    try {
      happnInstance.services.session.localClient(function(e, instance) {
        if (e) return callback(e);
        publisherclient = instance;

        happnInstance.services.session.localClient(function(e, instance) {
          if (e) return callback(e);
          listenerclient = instance;

          callback();
        });
      });
    } catch (e) {
      callback(e);
    }
  });

  it('the publisher should set new data', function(callback) {
    try {
      var test_path_end = require('shortid').generate();

      publisherclient.set(
        '1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/' + test_path_end,
        {
          property1: 'property1',
          property2: 'property2',
          property3: 'property3'
        },
        {
          noPublish: true
        },
        function(e, result) {
          if (!e) {
            publisherclient.get(
              '1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/' + test_path_end,
              null,
              function(e, results) {
                expect(results.property1 == 'property1').to.be(true);
                expect(results.created == results.modified).to.be(true);

                callback(e);
              }
            );
          } else callback(e);
        }
      );
    } catch (e) {
      callback(e);
    }
  });

  it('the listener should pick up a single wildcard event', function(callback) {
    try {
      //first listen for the change
      listenerclient.on(
        '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event/*',
        {
          event_type: 'set',
          count: 1
        },
        function(message) {
          expect(
            listenerclient.state.events[
              '/SET@/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event/*'
            ]
          ).to.be.undefined;
          callback();
        },
        function(e) {
          if (!e) {
            expect(
              listenerclient.state.events[
                '/SET@/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event/*'
              ].length
            ).to.be(1);

            //then make the change
            publisherclient.set(
              '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event/blah',
              {
                property1: 'property1',
                property2: 'property2',
                property3: 'property3'
              },
              null,
              function(e, result) {}
            );
          } else callback(e);
        }
      );
    } catch (e) {
      callback(e);
    }
  });

  it('the publisher should get null for unfound data, exact path', function(callback) {
    var test_path_end = require('shortid').generate();
    publisherclient.get(
      '1_eventemitter_embedded_sanity/' + test_id + '/unfound/exact/' + test_path_end,
      null,
      function(e, results) {
        ////////////console.log('new data results');

        expect(e).to.be(null);
        expect(results).to.be(null);

        callback(e);
      }
    );
  });

  it('set_multiple, the publisher should set multiple data items, then do a wildcard get to return them', function(callback) {
    var timesCount = 10;

    var testBasePath = '/1_eventemitter_embedded_sanity/' + test_id + '/set_multiple';

    try {
      async.times(
        timesCount,
        function(n, timesCallback) {
          var test_random_path2 = require('shortid').generate();

          publisherclient.set(
            testBasePath + '/' + test_random_path2,
            {
              property1: 'property1',
              property2: 'property2',
              property3: 'property3'
            },
            {
              noPublish: true
            },
            timesCallback
          );
        },
        function(e) {
          if (e) return callback(e);

          listenerclient.get(testBasePath + '/' + '*', null, function(e, results) {
            if (e) return callback(e);

            expect(results.length).to.be(timesCount);

            results.every(function(result) {
              /*
               RESULT SHOULD LOOK LIKE THIS
               { property1: 'property1',
               property2: 'property2',
               property3: 'property3',
               _meta:
               { modified: 1443606046766,
               created: 1443606046766,
               path: '/1_eventemitter_embedded_sanity/1443606046555_VkyH6cE1l/set_multiple/E17kSpqE1l' } }
               */

              expect(result.property1).to.be('property1');
              expect(result._meta.path.indexOf(testBasePath) == 0).to.be(true);

              return true;
            });

            callback();
          });
        }
      );
    } catch (e) {
      callback(e);
    }
  });

  it('should set data, and then merge a new document into the data without overwriting old fields', function(callback) {
    try {
      var test_path_end = require('shortid').generate();

      publisherclient.set(
        '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/merge/' + test_path_end,
        {
          property1: 'property1',
          property2: 'property2',
          property3: 'property3'
        },
        null,
        function(e, result) {
          if (e) return callback(e);

          publisherclient.set(
            '/1_eventemitter_embedded_sanity/' +
              test_id +
              '/testsubscribe/data/merge/' +
              test_path_end,
            {
              property4: 'property4'
            },
            {
              merge: true
            },
            function(e, result) {
              if (e) return callback(e);

              publisherclient.get(
                '/1_eventemitter_embedded_sanity/' +
                  test_id +
                  '/testsubscribe/data/merge/' +
                  test_path_end,
                null,
                function(e, results) {
                  if (e) return callback(e);

                  //////////////console.log('merge get results');
                  //////////////console.log(results);

                  expect(results.property4).to.be('property4');
                  expect(results.property1).to.be('property1');

                  callback();
                }
              );
            }
          );
        }
      );
    } catch (e) {
      callback(e);
    }
  });

  it('should contain the same payload between 2 non-merging consecutive stores', function(done) {
    var object = {
      param1: 10,
      param2: 20
    };
    var firstTimeNonMergeConsecutive;

    listenerclient.on(
      'setTest/nonMergeConsecutive',
      {
        event_type: 'set',
        count: 2
      },
      function(message, meta) {
        if (firstTimeNonMergeConsecutive === undefined) {
          firstTimeNonMergeConsecutive = message;
        } else {
          expect(message).to.eql(firstTimeNonMergeConsecutive);
          done();
        }
      },
      function(err) {
        expect(err).to.not.exist;
        publisherclient.set('setTest/nonMergeConsecutive', object, {}, function(err) {
          expect(err).to.not.be.ok();
          publisherclient.set('setTest/nonMergeConsecutive', object, {}, function(err) {
            expect(err).to.not.be.ok();
          });
        });
      }
    );
  });

  it('should contain the same payload between a merge and a normal store for first store', function(done) {
    var shortid = require('shortid').generate();

    var object = { param1: 10, param2: 20 };
    var firstTime = true;

    listenerclient.on(
      'mergeTest5/object/*',
      { event_type: 'set', count: 2 },
      function(message, meta) {
        expect(message).to.eql(object);

        if (firstTime) {
          firstTime = false;
          return;
        }
        done();
      },
      function(err) {
        if (err) return done(err);

        publisherclient.set('mergeTest5/object/' + shortid, object, { merge: true }, function(err) {
          if (err) return done(err);

          publisherclient.set('mergeTest5/object/' + shortid, object, { merge: true }, function(
            err
          ) {
            if (err) return done(err);
          });
        });
      }
    );
  });

  it('tests sift', function(callback) {
    var array = [{ value: 0 }, { value: 1 }, { value: 2 }];

    var sift = require('sift');

    var sifted = sift({ value: { $gte: 0, $lte: 2 } }, array);

    callback();
  });

  it('should delete some test data', function(callback) {
    try {
      //We put the data we want to delete into the database
      publisherclient.set(
        '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/delete_me',
        {
          property1: 'property1',
          property2: 'property2',
          property3: 'property3'
        },
        {
          noPublish: true
        },
        function(e, result) {
          //We perform the actual delete
          publisherclient.remove(
            '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/delete_me',
            {
              noPublish: true
            },
            function(e, result) {
              expect(e).to.be(null);
              expect(result._meta.status).to.be('ok');

              ////////////////////console.log('DELETE RESULT');
              ////////////////////console.log(result);

              callback();
            }
          );
        }
      );
    } catch (e) {
      callback(e);
    }
  });

  it('the publisher should set new data then update the data', function(callback) {
    try {
      var test_path_end = require('shortid').generate();

      publisherclient.set(
        '1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/' + test_path_end,
        {
          property1: 'property1',
          property2: 'property2',
          property3: 'property3'
        },
        {
          noPublish: true
        },
        function(e, insertResult) {
          expect(e).to.be(null);

          publisherclient.set(
            '1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/' + test_path_end,
            {
              property1: 'property1',
              property2: 'property2',
              property3: 'property3',
              property4: 'property4'
            },
            {
              noPublish: true
            },
            function(e, updateResult) {
              expect(e).to.be(null);
              expect(updateResult._meta.id == insertResult._meta.id).to.be(true);
              callback();
            }
          );
        }
      );
    } catch (e) {
      callback(e);
    }
  });

  it('should tag some test data', function(callback) {
    var randomTag = require('shortid').generate();

    publisherclient.set(
      '/1_eventemitter_embedded_sanity/' + test_id + '/test/tag',
      {
        property1: 'property1',
        property2: 'property2',
        property3: 'property3'
      },
      {
        noPublish: true
      },
      function(e, result) {
        if (e) return callback(e);

        publisherclient.set(
          '/1_eventemitter_embedded_sanity/' + test_id + '/test/tag',
          null,
          {
            tag: randomTag,
            merge: true,
            noPublish: true
          },
          function(e, result) {
            if (e) return callback(e);

            expect(result.data.property1).to.be('property1');
            expect(result.data.property2).to.be('property2');
            expect(result.data.property3).to.be('property3');

            setTimeout(function() {
              publisherclient.get(
                '/_TAGS/1_eventemitter_embedded_sanity/' + test_id + '/test/tag/*',
                null,
                function(e, results) {
                  if (e) return callback(e);

                  expect(results.length > 0).to.be(true);

                  var found = false;

                  results.every(function(tagged) {
                    if (tagged._meta.tag == randomTag) {
                      expect(tagged.data.property1).to.be('property1');
                      expect(tagged.data.property2).to.be('property2');
                      expect(tagged.data.property3).to.be('property3');

                      found = true;
                    }

                    return !found;
                  });

                  if (!found) callback("couldn't find the tag snapshot");
                  else callback();
                }
              );
            }, 1000);
          }
        );
      }
    );
  });

  //  We set the listener client to listen for a PUT event according to a path, then we set a value with the publisher client.

  it('the listener should pick up a single published event', function(callback) {
    try {
      //first listen for the change
      listenerclient.on(
        '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event',
        {
          event_type: 'set',
          count: 1
        },
        function(message) {
          expect(
            listenerclient.state.events[
              '/SET@/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event'
            ]
          ).to.be.undefined;
          callback();
        },
        function(e) {
          //////////////////console.log('ON HAS HAPPENED: ' + e);

          if (!e) {
            expect(
              listenerclient.state.events[
                '/SET@/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event'
              ].length
            ).to.be(1);
            //////////////////console.log('on subscribed, about to publish');

            //then make the change
            publisherclient.set(
              '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event',
              {
                property1: 'property1',
                property2: 'property2',
                property3: 'property3'
              },
              null,
              function(e, result) {
                ////////////////////////////console.log('put happened - listening for result');
              }
            );
          } else callback(e);
        }
      );
    } catch (e) {
      callback(e);
    }
  });

  //We are testing setting data at a specific path

  it('the publisher should set new data ', function(callback) {
    try {
      var test_path_end = require('shortid').generate();

      publisherclient.set(
        '1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/' + test_path_end,
        {
          property1: 'property1',
          property2: 'property2',
          property3: 'property3'
        },
        null,
        function(e, result) {
          if (!e) {
            publisherclient.get(
              '1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/' + test_path_end,
              null,
              function(e, results) {
                ////////////////////////console.log('new data results');
                ////////////////////////console.log(results);
                expect(results.property1 == 'property1').to.be(true);
                callback(e);
              }
            );
          } else callback(e);
        }
      );
    } catch (e) {
      callback(e);
    }
  });

  it('the publisher should set new data then update the data', function(callback) {
    try {
      var test_path_end = require('shortid').generate();

      publisherclient.set(
        '1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/' + test_path_end,
        {
          property1: 'property1',
          property2: 'property2',
          property3: 'property3'
        },
        null,
        function(e, insertResult) {
          expect(e == null).to.be(true);

          publisherclient.set(
            '1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/' + test_path_end,
            {
              property1: 'property1',
              property2: 'property2',
              property3: 'property3',
              property4: 'property4'
            },
            null,
            function(e, updateResult) {
              expect(e == null).to.be(true);
              expect(updateResult._meta._id == insertResult._meta._id).to.be(true);
              callback();
            }
          );
        }
      );
    } catch (e) {
      callback(e);
    }
  });

  //We are testing pushing a specific value to a path which will actually become an array in the database

  it('the publisher should push a sibling and get all siblings', function(callback) {
    try {
      var test_path_end = require('shortid').generate();

      publisherclient.setSibling(
        '1_eventemitter_embedded_sanity/' + test_id + '/siblings/' + test_path_end,
        {
          property1: 'sib_post_property1',
          property2: 'sib_post_property2'
        },
        function(e, results) {
          expect(e == null).to.be(true);

          publisherclient.setSibling(
            '1_eventemitter_embedded_sanity/' + test_id + '/siblings/' + test_path_end,
            {
              property1: 'sib_post_property1',
              property2: 'sib_post_property2'
            },
            function(e, results) {
              expect(e == null).to.be(true);

              //the child method returns a child in the collection with a specified id
              publisherclient.get(
                '1_eventemitter_embedded_sanity/' + test_id + '/siblings/' + test_path_end + '/*',
                null,
                function(e, getresults) {
                  expect(e == null).to.be(true);
                  expect(getresults.length == 2).to.be(true);
                  callback(e);
                }
              );
            }
          );
        }
      );
    } catch (e) {
      callback(e);
    }
  });

  // We set the listener client to listen for a PUT event according to a path, then we set a value with the publisher client.

  it('the listener should pick up a single published event', function(callback) {
    try {
      //first listen for the change
      listenerclient.on(
        '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event',
        {
          event_type: 'set',
          count: 1
        },
        function(message) {
          expect(
            listenerclient.state.events[
              '/SET@/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event'
            ]
          ).to.be.undefined;
          callback();
        },
        function(e) {
          if (!e) {
            expect(
              listenerclient.state.events[
                '/SET@/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event'
              ].length
            ).to.be(1);

            ////////////////////////////console.log('on subscribed, about to publish');

            //then make the change
            publisherclient.set(
              '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event',
              {
                property1: 'property1',
                property2: 'property2',
                property3: 'property3'
              },
              null,
              function(e, result) {
                ////////////////////////////console.log('put happened - listening for result');
              }
            );
          } else callback(e);
        }
      );
    } catch (e) {
      callback(e);
    }
  });

  it('should get using a wildcard', function(callback) {
    var test_path_end = require('shortid').generate();

    publisherclient.set(
      '1_eventemitter_embedded_sanity/' + test_id + '/testwildcard/' + test_path_end,
      {
        property1: 'property1',
        property2: 'property2',
        property3: 'property3'
      },
      null,
      function(e, insertResult) {
        expect(e == null).to.be(true);
        publisherclient.set(
          '1_eventemitter_embedded_sanity/' + test_id + '/testwildcard/' + test_path_end + '/1',
          {
            property1: 'property1',
            property2: 'property2',
            property3: 'property3'
          },
          null,
          function(e, insertResult) {
            expect(e == null).to.be(true);

            publisherclient.get(
              '1_eventemitter_embedded_sanity/' + test_id + '/testwildcard/' + test_path_end + '*',
              null,
              function(e, results) {
                if (e) return callback();

                expect(results.length == 2).to.be(true);
                callback(e);
              }
            );
          }
        );
      }
    );
  });

  it('should get paths', function(callback) {
    var test_path_end = require('shortid').generate();

    publisherclient.set(
      '1_eventemitter_embedded_sanity/' + test_id + '/testwildcard/' + test_path_end,
      {
        property1: 'property1',
        property2: 'property2',
        property3: 'property3'
      },
      null,
      function(e, insertResult) {
        expect(e == null).to.be(true);
        publisherclient.set(
          '1_eventemitter_embedded_sanity/' + test_id + '/testwildcard/' + test_path_end + '/1',
          {
            property1: 'property1',
            property2: 'property2',
            property3: 'property3'
          },
          null,
          function(e, insertResult) {
            expect(e == null).to.be(true);

            publisherclient.getPaths(
              '1_eventemitter_embedded_sanity/' + test_id + '/testwildcard/' + test_path_end + '*',
              function(e, results) {
                expect(results.length == 2).to.be(true);
                callback(e);
              }
            );
          }
        );
      }
    );
  });

  it('the listener should pick up a single delete event', function(callback) {
    //We put the data we want to delete into the database
    publisherclient.set(
      '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/delete_me',
      {
        property1: 'property1',
        property2: 'property2',
        property3: 'property3'
      },
      null,
      function(e, result) {
        //////////////////console.log('did delete set');
        //path, event_type, count, handler, done
        //We listen for the DELETE event
        listenerclient.on(
          '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/delete_me',
          {
            event_type: 'remove',
            count: 1
          },
          function(eventData) {
            //we are looking at the event internals on the listener to ensure our event management is working - because we are only listening for 1
            //instance of this event - the event listener should have been removed
            expect(
              listenerclient.events[
                '/REMOVE@/1_eventemitter_embedded_sanity/' +
                  test_id +
                  '/testsubscribe/data/delete_me'
              ].length
            ).to.be(0);

            //we needed to have removed a single item
            expect(eventData.payload.removed).to.be(1);

            callback();
          },
          function(e) {
            if (!e) return callback(e);

            expect(
              listenerclient.events[
                '/REMOVE@/1_eventemitter_embedded_sanity/' +
                  test_id +
                  '/testsubscribe/data/delete_me'
              ].length
            ).to.be(1);

            //We perform the actual delete
            publisherclient.remove(
              '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/delete_me',
              null,
              function(e, result) {}
            );
          }
        );
      }
    );
  });

  it('should unsubscribe from an event', function(callback) {
    var currentListenerId;

    listenerclient.on(
      '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/on_off_test',
      {
        event_type: 'set',
        count: 0
      },
      function(message) {
        //we detach all listeners from the path here
        ////console.log('ABOUT OFF PATH');
        listenerclient.offPath(
          '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/on_off_test',
          function(e) {
            if (e) return callback(new Error(e));

            listenerclient.on(
              '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/on_off_test',
              {
                event_type: 'set',
                count: 0
              },
              function(message) {
                ////console.log('ON RAN');
                ////console.log(message);

                listenerclient.off(currentListenerId, function(e) {
                  if (e) return callback(new Error(e));
                  else return callback();
                });
              },
              function(e, listenerId) {
                if (e) return callback(new Error(e));

                currentListenerId = listenerId;

                publisherclient.set(
                  '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/on_off_test',
                  {
                    property1: 'property1',
                    property2: 'property2',
                    property3: 'property3'
                  },
                  {},
                  function(e) {
                    if (e) return callback(new Error(e));
                  }
                );
              }
            );
          }
        );
      },
      function(e, listenerId) {
        if (e) return callback(new Error(e));

        currentListenerId = listenerId;

        publisherclient.set(
          '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/on_off_test',
          {
            property1: 'property1',
            property2: 'property2',
            property3: 'property3'
          },
          {},
          function(e, setresult) {
            if (e) return callback(new Error(e));
          }
        );
      }
    );
  });

  it('should subscribe to the catch all notification', function(callback) {
    var caught = {};

    this.timeout(10000);
    var caughtCount = 0;

    listenerclient.onAll(
      function(eventData, meta) {
        if (
          meta.action ==
            '/REMOVE@/1_eventemitter_embedded_sanity/' +
              test_id +
              '/testsubscribe/data/catch_all' ||
          meta.action ==
            '/SET@/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/catch_all'
        )
          caughtCount++;

        if (caughtCount == 2) callback();
      },
      function(e) {
        if (e) return callback(e);

        publisherclient.set(
          '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/catch_all',
          {
            property1: 'property1',
            property2: 'property2',
            property3: 'property3'
          },
          null,
          function(e, put_result) {
            publisherclient.remove(
              '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/catch_all',
              null,
              function(e, del_result) {}
            );
          }
        );
      }
    );
  });

  it('should unsubscribe from all events', function(callback) {
    this.timeout(10000);

    var onHappened = false;

    listenerclient.onAll(
      function(message) {
        onHappened = true;
        callback(new Error('this wasnt meant to happen'));
      },
      function(e) {
        if (e) return callback(e);

        listenerclient.on(
          '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/off_all_test',
          {
            event_type: 'set',
            count: 0
          },
          function(message) {
            onHappened = true;
            callback(new Error('this wasnt meant to happen'));
          },
          function(e) {
            if (e) return callback(e);

            listenerclient.offAll(function(e) {
              if (e) return callback(e);

              publisherclient.set(
                '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/off_all_test',
                {
                  property1: 'property1',
                  property2: 'property2',
                  property3: 'property3'
                },
                null,
                function(e, put_result) {
                  if (e) return callback(e);

                  setTimeout(function() {
                    if (!onHappened) callback();
                  }, 3000);
                }
              );
            });
          }
        );
      }
    );
  });

  it('should not publish with noPublish set', function(done) {
    var timeout;
    //first listen for the change
    listenerclient.on(
      '/1_eventemitter_embedded_sanity/' + test_id + '/testNoPublish',
      {
        event_type: 'set',
        count: 1
      },
      function(message) {
        clearTimeout(timeout);
        setImmediate(function() {
          expect(message).to.not.be.ok();
        });
      },
      function(e) {
        expect(e).to.not.be.ok();

        timeout = setTimeout(function() {
          listenerclient.offPath(
            '/1_eventemitter_embedded_sanity/' + test_id + '/testNoPublish',
            function() {
              done();
            }
          );
        }, 1000);
        publisherclient.set(
          '/1_eventemitter_embedded_sanity/' + test_id + '/testNoPublish',
          {
            property1: 'property1',
            property2: 'property2',
            property3: 'property3'
          },
          {
            noPublish: true
          },
          function(e, result) {
            expect(e).to.not.be.ok();
          }
        );
      }
    );
  });

  it('will do events in the order they are passed', function(done) {
    publisherclient.set('/test_event_order', { property1: 'property1Value' }, {}, function() {
      publisherclient.log.info('Done setting');
    });
    publisherclient.remove('/test_event_order', function(err) {
      publisherclient.log.info('Done removing');
      setTimeout(function() {
        publisherclient.get('/test_event_order', null, function(e, result) {
          expect(result).to.be(null);
          done();
        });
      }, 1000);
    });
  });

  it('the publisher should set and get spaces in the path', async () => {
    var test_path = `/complex spaces get set/${test_id}`;

    await publisherclient.set(
      test_path,
      {
        property1: 'property1',
        property2: 'property2',
        property3: 'property3'
      },
      {
        noPublish: true
      }
    );
    const results = await publisherclient.get(test_path);
    expect(results.property1 === 'property1').to.be(true);
    expect(results._meta.created === results._meta.modified).to.be(true);
  });

  it('should search for a complex object, with spaces in the path', async () => {
    var test_path = `/complex spaces/${test_id}`;

    var complex_obj = {
      regions: ['North', 'South'],
      towns: ['North.Cape Town'],
      categories: ['Action', 'History'],
      subcategories: ['Action.angling', 'History.art'],
      keywords: ['bass', 'Penny Siopis'],
      field1: 'field1'
    };

    var criteria1 = {
      $or: [
        {
          'data.regions': {
            $in: ['North', 'South', 'East', 'West']
          }
        },
        {
          'data.towns': {
            $in: ['North.Cape Town', 'South.East London']
          }
        },
        {
          'data.categories': {
            $in: ['Action', 'History']
          }
        }
      ],
      'data.keywords': {
        $in: ['bass', 'Penny Siopis']
      }
    };

    var options1 = {
      sort: {
        'data.teststamp': 1
      },
      limit: 1
    };

    var criteria2 = null;

    var options2 = {
      fields: { towns: 1, keywords: 1 },
      sort: {
        'data.teststamp': 1
      },
      limit: 2
    };

    var options3 = {
      sort: {
        'data.teststamp': -1
      }
    };

    var options4 = {
      sort: {
        'data.teststamp': 1
      }
    };

    const teststamp1 = Date.now();
    await delay(5);
    const teststamp2 = Date.now();

    await publisherclient.set(`${test_path}/0`, { ...complex_obj, teststamp: teststamp1 });
    await publisherclient.set(`${test_path}/1`, { ...complex_obj, teststamp: teststamp2 });

    const results1 = await publisherclient.get(`${test_path}/*`, {
      criteria: criteria1,
      options: options1
    });

    const results2 = await publisherclient.get(`${test_path}/*`, {
      criteria: criteria2,
      options: options2
    });

    const results3 = await publisherclient.get(`${test_path}/*`, {
      options: options3
    });

    const results4 = await publisherclient.get(`${test_path}/*`, {
      options: options4
    });

    expect(results1.length === 1).to.be(true);
    expect(results2.length === 2).to.be(true);

    expect(results1[0].teststamp).to.be(teststamp1);
    expect(results3[0].teststamp).to.be(teststamp2);
    expect(results3.length).to.be(2);
    expect(results4[0].teststamp).to.be(teststamp1);
  });

  it('should search for a complex object', async () => {
    var complex_obj = {
      regions: ['North', 'South'],
      towns: ['North.Cape Town'],
      categories: ['Action', 'History'],
      subcategories: ['Action.angling', 'History.art'],
      keywords: ['bass', 'Penny Siopis'],
      field1: 'field1',
      timestamp: Date.now()
    };

    var criteria1 = {
      $or: [
        {
          'data.regions': {
            $in: ['North', 'South', 'East', 'West']
          }
        },
        {
          'data.towns': {
            $in: ['North.Cape Town', 'South.East London']
          }
        },
        {
          'data.categories': {
            $in: ['Action', 'History']
          }
        }
      ],
      'data.keywords': {
        $in: ['bass', 'Penny Siopis']
      }
    };

    var options1 = {
      sort: {
        'data.timestamp': 1
      },
      limit: 1
    };

    var criteria2 = null;

    var options2 = {
      fields: { towns: 1, keywords: 1 },
      sort: {
        'data.timestamp': 1
      },
      limit: 2
    };

    var options3 = {
      fields: { towns: 1, keywords: 1 },
      sort: {
        'data.timestamp': 1
      },
      limit: 3
    };

    await publisherclient.set(`/complexsearch/${test_id}/0`, complex_obj);
    await publisherclient.set(`/complexsearch/${test_id}/1`, complex_obj);
    await publisherclient.set(`/complexsearch/${test_id}/2`, complex_obj);

    const result1 = await publisherclient.get(`/complexsearch/${test_id}/*`, {
      criteria: criteria1,
      options: options1
    });

    const result2 = await publisherclient.get(`/complexsearch/${test_id}/*`, {
      criteria: criteria2,
      options: options2
    });

    const result3 = await publisherclient.get(`/complexsearch/${test_id}/*`, {
      criteria: criteria2,
      options: options3
    });

    expect(result1.length === 1).to.be(true);
    expect(result2.length === 2).to.be(true);
    expect(result3.length === 3).to.be(true);
  });

  it('should search for a complex object by dates', function(callback) {
    var test_path_end = require('shortid').generate();

    var complex_obj = {
      dregions: ['North', 'South'],
      towns: ['North.Cape Town'],
      categories: ['Action', 'History'],
      subcategories: ['Action.angling', 'History.art'],
      keywords: ['bass', 'Penny Siopis'],
      field1: 'field1'
    };

    var from = Date.now();

    var to;

    publisherclient.set(
      '/1_eventemitter_embedded_sanity/' +
        test_id +
        '/testsubscribe/data/complex/by/date/' +
        test_path_end,
      complex_obj,
      null,
      function(e) {
        expect(e == null).to.be(true);

        setTimeout(function() {
          to = Date.now();

          var criteria = {
            '_meta.created': {
              $gte: from,
              $lte: to
            }
          };

          var options = {
            fields: null,
            sort: {
              'data.field1': 1
            },
            limit: 2
          };

          publisherclient.get(
            '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/complex/by/date/*',
            {
              criteria: criteria,
              options: options
            },
            function(e, search_result) {
              expect(e == null).to.be(true);

              if (search_result.length == 0) {
                return callback(new Error('no items found in the date range'));
              } else {
                return callback();
              }
            }
          );
        }, 1000);
      }
    );
  });

  it('should bulk index some data', function(done) {
    var dynamicType = (test_id + 'type').toLowerCase();

    var dynamicIndex = (test_id + 'index').toLowerCase();

    var bulkItems = [
      {
        data: {
          indexProperty: dynamicIndex,
          typeProperty: dynamicType,
          test: 1
        }
      },
      {
        data: {
          indexProperty: dynamicIndex,
          typeProperty: dynamicType,
          test: 2
        }
      },
      {
        data: {
          indexProperty: dynamicIndex,
          typeProperty: dynamicType,
          test: 3
        }
      },
      {
        data: {
          indexProperty: dynamicIndex,
          typeProperty: dynamicType,
          test: 4
        }
      }
    ];

    publisherclient.set(
      '/dynamic/{{indexProperty}}/{{typeProperty}}/{id}',
      bulkItems,
      { upsertType: 3 },
      function(e, inserted) {
        if (e) return done(e);

        try {
          expect(inserted._meta.errors).to.be(false);
          expect(inserted._meta.items.length).to.be(4);

          for (var i = 0; i < inserted.value.length; i++)
            expect(inserted.value[i].data.indexProperty).to.be(dynamicIndex);

          done();
        } catch (e) {
          done(e);
        }
      }
    );
  });

  it('should fail bad index and type', function(done) {
    var dynamicType = test_id + 'TYPE';

    var dynamicIndex = test_id + 'INDEX';

    var bulkItems = [
      {
        data: {
          indexProperty: dynamicIndex,
          typeProperty: dynamicType,
          test: 1
        }
      },
      {
        data: {
          indexProperty: dynamicIndex,
          typeProperty: dynamicType,
          test: 2
        }
      },
      {
        data: {
          indexProperty: dynamicIndex,
          typeProperty: dynamicType,
          test: 3
        }
      },
      {
        data: {
          indexProperty: dynamicIndex,
          typeProperty: dynamicType,
          test: 4
        }
      }
    ];

    publisherclient.set(
      '/dynamic/{{indexProperty}}/{{typeProperty}}/{id}',
      bulkItems,
      { upsertType: 3 },
      function(e) {
        try {
          expect(e).to.not.be(null);
          expect(e).to.not.be(undefined);
          return done();
        } catch (e) {
          done(e);
        }
      }
    );
  });
});
