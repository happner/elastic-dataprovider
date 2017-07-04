describe('func-indexes', function() {

  this.timeout(5000);

  var expect = require('expect.js');

  var service = require('..');

  var testId = require('shortid').generate();

  var path = require('path');

  var provider_path = path.resolve('../index.js');

  var config = {
    name:'elastic',
    provider:provider_path,
    defaultIndex:"indextest",
    host:"http://localhost:9200",
    indexes:[
    {
      index: "indextest",
      body: {
        "mappings": {}
      }
    },
    {
      index: "custom"
    }],
    dataroutes:[
      {pattern:"/custom/*",index:"custom"},
      {pattern:"*",index:"indextest"}
    ]
  };


  var serviceInstance = new service(config);

  before('should initialize the service', function(callback) {

    if (!serviceInstance.happn)
      serviceInstance.happn = {
        services:{
          utils:{
            wildcardMatch:function (pattern, matchTo) {

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

  var getElasticClient = function(callback){

    var elasticsearch = require('elasticsearch');

    try{

      var client = new elasticsearch.Client({"host":"localhost:9200"});

      client.ping({
        requestTimeout: 30000
      }, function (e) {

        if (e) return callback(e);

        callback(null, client);
      });
    }catch(e){
      callback(e);
    }
  };

  var listAll = function(client, index, type, callback){

    var elasticMessage = {
      "index": index,
      "type":  type,
      "body":{
        "from":0,
        "size":10000
      }
    };

    client.search(elasticMessage)

      .then(function (resp) {

        if (resp.hits && resp.hits.hits && resp.hits.hits.length > 0){

          callback(null, resp.hits.hits);

        } else callback(null, []);

      })
      .catch(function(e){
        callback(e);
      })
  };

  it('sets data with custom path, and data with default path, we then query the data directly and ensure our counts are right', function(done) {

    serviceInstance.upsert('/custom/' + testId, {data:{"test":"custom"}}, {}, false, function(e, response, created){

      if (e) return done(e);

      serviceInstance.upsert('/default/' + testId, {data:{"test":"default"}}, {}, false, function(e, response, created){

        if (e) return done(e);

        getElasticClient(function(e, client){

          if (e) return done(e);

          var foundItems = [];

          setInterval(function(){

            listAll(client, "indextest", "happner", function(e, defaultItems){

              if (e) return done(e);

              listAll(client, "custom", "happner", function(e, customItems){

                if (e) return done(e);

                defaultItems.forEach(function(item){
                  if (item._id == '/default/' + testId)  foundItems.push(item);
                });

                expect(foundItems.length).to.be(1);

                foundItems = [];

                customItems.forEach(function(item){
                  if (item._id == '/custom/' + testId)  foundItems.push(item);
                });

                expect(foundItems.length).to.be(1);

                done();
              });
            });

          }, 1000);
        });
      });
    });
  });
});