var mongoToElastic = require('./lib/mongo-to-elastic')
  ,  sift = require('sift')
  , async = require('async')
;

function ElasticProvider (config){

  if (config.cache) {

    if (config.cache === true) config.cache = {};

    if (!config.cache.cacheId) config.cache.cacheId = config.name;
  }

  if (!config.defaultIndex) config.defaultIndex = 'happner';

  if (!config.defaultType) config.defaultType = 'happner';

  Object.defineProperty(this, 'config', {value:config});
}

ElasticProvider.prototype.__getIndex = function(path){

  var _this = this;

  var index = _this.config.defaultIndex;

  _this.config.dataroutes.every(function(dataStoreRoute){

    if (_this.happn.services.utils.wildcardMatch(dataStoreRoute.pattern, path)){
      index = dataStoreRoute.index;
      return false;
    }
    else return true;
  });

  return index;
};


ElasticProvider.prototype.wildcardMatch = function (pattern, matchTo) {
  return matchTo.match(new RegExp(pattern.replace(/[*]/g, '.*'))) != null;
};

ElasticProvider.prototype.__createIndex = function(index, indexConfig, callback){

  var _this = this;

  var doCallback = function(e, response){
    if (e) return callback(new Error('failed creating index' + index + ':' + e.toString(), e));
    callback(null, response);
  };

  _this.db.indices.exists({
    'index':	index
  }, function (e, res){

    if (e) return doCallback(e);

    if (res === false) {

      _this.db.indices.create(indexConfig, doCallback);

    } else return doCallback(null, res);
  });
};

ElasticProvider.prototype.__createIndexes = function(callback){

  var _this = this;

  if (!_this.config.indexes) _this.config.indexes = [];

  var defaultIndexFound = false;

  _this.config.indexes.forEach(function(indexConfig){

    if (indexConfig.index == _this.config.defaultIndex) defaultIndexFound = true;
  });

  var indexJSON = {
    index: _this.config.defaultIndex,
    body: {
      "mappings": {}
    }
  };

  var typeJSON = {
    "properties": {
      "path":       {"type": "keyword"},
      "data":       {"type": "object"},
      "created":    {"type": "date"},
      "timestamp":  {"type": "date"},
      "modified":   {"type": "date"},
      "modifiedBy": {"type":"keyword"},
      "createdBy":  {"type":"keyword"}
    }
  };

  indexJSON.body.mappings[_this.config.defaultType] = typeJSON;

  if (!defaultIndexFound){
    _this.config.indexes.push(indexJSON);
  }
  
  async.eachSeries(_this.config.indexes, function(index, indexCB){

    if (index.index != _this.defaultIndex) {

      if (index.body == null) index.body = {};

      if (index.body.mappings == null) index.body.mappings = {};

      if (index.body.mappings[_this.config.defaultType] == null) index.body.mappings[_this.config.defaultType] = {};

      Object.keys(indexJSON.body.mappings[_this.config.defaultType]).forEach(function(fieldName){

        if (index.body.mappings[_this.config.defaultType][fieldName] == null)
          index.body.mappings[_this.config.defaultType][fieldName] = indexJSON.body.mappings[_this.config.defaultType][fieldName];
      });
    }

    _this.__createIndex(index.index, index, indexCB);

  }, function(e){

    if (e) return callback(e);

    if (!_this.config.dataroutes) _this.config.dataroutes = [];

    //last route goes to default index

    var defaultRouteFound = false;

    _this.config.dataroutes.forEach(function(route){
      if (route.pattern == '*') defaultRouteFound = true;
    });

    if (!defaultRouteFound) _this.config.dataroutes.push({pattern:'*', index:_this.config.defaultIndex});

    callback();
  });
};

ElasticProvider.prototype.initialize = function(callback){

  var _this = this;

  var elasticsearch = require('elasticsearch');

  try{

    var client = new elasticsearch.Client(_this.config);

    client.ping({
      requestTimeout: 30000
    }, function (e) {

      if (e) return callback(e);

      Object.defineProperty(_this, 'db', {value:client});

      _this.__createIndexes(callback);
    });
  }catch(e){
    callback(e);
  }
};

ElasticProvider.prototype.findOne = function(criteria, fields, callback){

  this.find(criteria, fields, function(e, results){
    if (e) return callback(e);

    callback(null, results[0]);
  })
};

ElasticProvider.prototype.sanitize = function (query) {

  return query
    .replace(/[\*\+\-=~><\"\?^\${}\(\)\:\!\/[\]\\\s]/g, '\\$&') // replace single character special characters
    .replace(/\|\|/g, '\\||') // replace ||
    .replace(/\&\&/g, '\\&&') // replace &&
    .replace(/AND/g, '\\A\\N\\D') // replace AND
    .replace(/OR/g, '\\O\\R') // replace OR
    .replace(/NOT/g, '\\N\\O\\T'); // replace NOT
};

ElasticProvider.prototype.__filter = function(criteria, items){
  try{
    return sift(criteria, items);
  }catch(e){
    throw new Error('filter failed: ' + e.toString(), e);
  }
};

ElasticProvider.prototype.find = function(path, parameters, callback){

  var _this = this;

  var elasticMessage = {
    "index": _this.__getIndex(path),
    "type":  _this.config.defaultType,
    "body":{
      "query":{
        "bool":{
          "must":[]
        }
      }
    }
  };

  if (parameters.options) mongoToElastic.convertOptions(parameters.options, elasticMessage);//this is because the $not keyword works in nedb and sift, but not in elastic

  if (elasticMessage.body.from == null) elasticMessage.body.from = 0;

  if (elasticMessage.body.size == null) elasticMessage.body.size = 10000;

  if (path.indexOf('*') > -1){

    elasticMessage.body["query"]["bool"]["must"].push({
      "wildcard": {
        "path": path
      }
    });

  } else {

    elasticMessage.body["query"]["bool"]["must"].push({
      "terms": {
        "_id": [ path ]
      }
    });
  }

  this.db.search(elasticMessage)

    .then(function (resp) {

      if (resp.hits && resp.hits.hits && resp.hits.hits.length > 0){

        var items = _this.__transformResults(resp.hits.hits);

        if (parameters.criteria) items = _this.__filter(parameters.criteria, items);

        callback(null, items);

      } else callback(null, []);

    })
    .catch(function(e){
      callback(e);
    })
};

ElasticProvider.prototype.update = function(criteria, data, options, callback){

  return this.db.update(criteria, data, options, callback);
};

ElasticProvider.prototype.__transformResult = function(result){

  var transformed = {};

  transformed._meta = this.__getMeta(result._source);
  transformed.data = result._source.data;

  return transformed;
};

ElasticProvider.prototype.__transformResults = function(results){

  var _this = this;
  var transformed = [];

  results.forEach(function(result){
    transformed.push(_this.__transformResult(result));
  });

  return transformed;
};

ElasticProvider.prototype.__getMeta = function(response){

  var meta = {
    created:response.created,
    modified:response.modified,
    modifiedBy:response.modifiedBy,
    timestamp:response.timestamp,
    path:response.path,
    _id:response.path
  };

  return meta;
};

ElasticProvider.prototype.upsert = function(path, setData, options, dataWasMerged, callback){

  var _this = this;

  var modifiedOn = Date.now();

  var timestamp = setData.data.timestamp?setData.data.timestamp:modifiedOn;

  var index = _this.__getIndex(path);

  var elasticMessage = {
    "index": index,
    "type":  _this.config.defaultType,
    id: path,
    body:{
      doc:{
        modified:modifiedOn,
        timestamp:timestamp,
        path:path,
        data:setData.data
      },
      upsert:{
        created:modifiedOn,
        modified:modifiedOn,
        timestamp:timestamp,
        path:path,
        data:setData.data
      }
    },
    _source: true,
    refresh:true
  };

  if (options.modifiedBy) {

    elasticMessage.body.doc.modifiedBy = options.modifiedBy;
    elasticMessage.body.upsert.createdBy = options.modifiedBy;
  }

  if (setData._tag) elasticMessage.body.doc._tag = setData._tag;

  if (!options) options = {};

  options.upsert = true;

  _this.db.update(elasticMessage, function (e, response) {

    if (e) return callback(e);

    var data = response.get._source;

    var created = null;

    if (response.result == 'created') created = data;
    //e, response, created, upsert, meta
    callback(null, data, created, true, _this.__getMeta(response.get._source));
  });
};

ElasticProvider.prototype.count = function(message, callback){

  var countMessage = {
    index:message.index,
    type:message.type
  };

  if (message.id) countMessage.body = {
    "query":{
      "match": {
        "path": message.id
      }
    }
  };

  else if (message.body) countMessage.body = message.body;

  this.db.count(countMessage, function(e, response){

    if (e) return callback(e);

    callback(null, response.count);
  });
};

ElasticProvider.prototype.remove = function(path, callback){

  var _this = this;

  var multiple = path.indexOf('*') > -1;
  var deletedCount = 0;

  var elasticMessage = {
    index: _this.__getIndex(path),
    type:  _this.config.defaultType,
    refresh:true
  };

  var handleResponse = function(e, response){

    if (e) return callback(e);

    var deleteResponse = {
      "data": {
        "removed": deletedCount
      },
      '_meta': {
        "timestamp": Date.now(),
        "path": path
      }
    };

    callback(null, deleteResponse);
  };

  if (multiple){

    elasticMessage.body = {
      "query":{
        "wildcard": {
          "path": path
        }
      }
    };

    //deleteOperation = this.db.deleteByQuery.bind(this.db);

  } else elasticMessage.id = path;

  _this.count(elasticMessage, function(e, count){

    if (e) return callback(new Error('count operation failed for delete: ' + e.toString()));

    deletedCount = count;

    if (multiple) _this.db.deleteByQuery(elasticMessage, handleResponse);

    else _this.db.delete(elasticMessage, handleResponse);
  });
};

ElasticProvider.prototype.startCompacting = function (interval, callback, compactionHandler) {
  return callback();
};

ElasticProvider.prototype.stopCompacting = function (callback) {
  return callback();
};

ElasticProvider.prototype.compact = function (callback) {
  return callback();
};

ElasticProvider.prototype.stop = function(callback){
  this.db.close();
  callback();
};

module.exports = ElasticProvider;
