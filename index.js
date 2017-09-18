var mongoToElastic = require('./lib/mongo-to-elastic')
  , sift = require('sift')
  , async = require('async')
  , traverse = require('traverse')
  , Comedian = require('co-median')
  , Cache = require('redis-lru-cache')
;

function ElasticProvider(config) {

  if (config.cache) {

    if (config.cache === true) config.cache = {};

    if (!config.cache.cacheId) config.cache.cacheId = config.name;
  }

  if (!config.defaultIndex) config.defaultIndex = 'happner';

  if (!config.defaultType) config.defaultType = 'happner';

  if (!config.wildcardCache) config.wildcardCache = {cache: 1000};

  if (!config.elasticCallConcurrency) config.elasticCallConcurrency = 100;

  Object.defineProperty(this, '__config', {value: config});

  Object.defineProperty(this, '__dynamicRoutes', {value: {}});

  Object.defineProperty(this, '__comedian', {value: new Comedian(config.wildcardCache)});

}

ElasticProvider.prototype.UPSERT_TYPE = {
  upsert:0,
  update:1,
  insert:2,
  bulk:3
};


/* initialize and stop */

ElasticProvider.prototype.initialize = initialize;

ElasticProvider.prototype.__initializeRoutes = __initializeRoutes;

ElasticProvider.prototype.stop = stop;


/* upsert, insert, update and remove */

ElasticProvider.prototype.upsert = upsert;

ElasticProvider.prototype.__ensureDynamic = __ensureDynamic;

ElasticProvider.prototype.__createDynamicIndex = __createDynamicIndex;

ElasticProvider.prototype.__insert = __insert;

ElasticProvider.prototype.__bulk = __bulk;

ElasticProvider.prototype.__update = __update;

ElasticProvider.prototype.__createBulkMessage = __createBulkMessage;

ElasticProvider.prototype.remove = remove;


/* find and count */

ElasticProvider.prototype.find = find;

ElasticProvider.prototype.findOne = findOne;

ElasticProvider.prototype.count = count;

ElasticProvider.prototype.__filter = __filter;


  /* data transformation and fields */

ElasticProvider.prototype.__partialTransformAll = __partialTransformAll;

ElasticProvider.prototype.__partialTransform = __partialTransform;

ElasticProvider.prototype.__partialInsertTransform = __partialInsertTransform;

ElasticProvider.prototype.transform = transform;

ElasticProvider.prototype.transformAll = transformAll;

ElasticProvider.prototype.__parseFields = __parseFields;

ElasticProvider.prototype.__getMeta = __getMeta;


/* caches */

ElasticProvider.prototype.setUpCache = setUpCache;

/* routes and dynamic data */

ElasticProvider.prototype.__matchRoute = __matchRoute;

ElasticProvider.prototype.__getRoute = __getRoute;

ElasticProvider.prototype.__getDynamicParts = __getDynamicParts;

/* indexes */

ElasticProvider.prototype.__createIndex = __createIndex;

ElasticProvider.prototype.__buildIndexObj = __buildIndexObj;

ElasticProvider.prototype.__createIndexes = __createIndexes;

/* elastic message queue */


ElasticProvider.prototype.__executeElasticAdminMessage = __executeElasticAdminMessage;

ElasticProvider.prototype.__executeElasticMessage = __executeElasticMessage;

/* utility methods */

ElasticProvider.prototype.__wildcardMatch = __wildcardMatch;

ElasticProvider.prototype.escapeRegex = escapeRegex;

ElasticProvider.prototype.preparePath = preparePath;


function initialize (callback) {

  var _this = this;

  var elasticsearch = require('elasticsearch');

  try {

    _this.__initializeRoutes();

    //yada yada yada: https://github.com/elastic/elasticsearch-js/issues/196
    var AgentKeepAlive = require('agentkeepalive');

    _this.__config.createNodeAgent = function (connection, config) {
      return new AgentKeepAlive(connection.makeAgentConfig(config));
    };

    var client = new elasticsearch.Client(_this.__config);

    client.ping({
      requestTimeout: 30000
    }, function (e) {

      if (e) return callback(e);

      Object.defineProperty(_this, 'db', {value: client});

      Object.defineProperty(_this, '__elasticCallQueue', {value: async.queue(_this.__executeElasticMessage.bind(_this), _this.__config.elasticCallConcurrency)});

      Object.defineProperty(_this, '__elasticAdminCallQueue', {value: async.queue(_this.__executeElasticAdminMessage.bind(_this), 1)});

      if (_this.__config.cache) _this.setUpCache();

      _this.__createIndexes(callback);

    });
  } catch (e) {
    callback(e);
  }
}

function stop (callback) {

  this.db.close();

  callback();
}


function __initializeRoutes () {

  var _this = this;

  _this.__config.dataroutes.forEach(function (route) {

    if (!route.index) route.index = _this.__config.defaultIndex;
    if (!route.type) route.type = _this.__config.defaultType;
  });
}

function upsert (path, setData, options, dataWasMerged, callback) {

  var _this = this;

  //[start:{"key":"upsert", "self":"_this"}:start]

  try{

    if (Array.isArray(path)){//check for bulk upsert

      callback = dataWasMerged;
      options = setData;
      setData = path;

      options.upsertType = _this.UPSERT_TYPE.bulk;

      return _this.__bulk(setData, options, callback);
    }

    var modifiedOn = Date.now();

    var timestamp = setData.data.timestamp ? setData.data.timestamp : modifiedOn;

    if (!options) options = {};

    if (options.refresh == null) options.refresh = true; //slow but reliable

    if (options.upsertType == null) options.upsertType = _this.UPSERT_TYPE.upsert;

    if (options.retries == null) options.retries = 20;

    var route = _this.__getRoute(path, options);

    if (options.upsertType == _this.UPSERT_TYPE.insert)//dynamic index is generated automatically using "index"
      return _this.__insert(path, setData, options, route, timestamp, modifiedOn, callback);

    _this.__ensureDynamic (route)//upserting so we need to make sure our index exists

      .then(function(){

        //[end:{"key":"upsert", "self":"_this"}:end]
        _this.__update(path, setData, options, route, timestamp, modifiedOn, callback);
      })

      .catch(callback);

  }catch(e){

    callback(e);
  }
}

var dynamicRoutes = {};

function __ensureDynamic(route){

  var _this = this;

  return new Promise(function(resolve, reject){

    try{

      if (!route.dynamic || dynamicRoutes[route.index]) return resolve();

      _this.__createDynamicIndex(route, function(e){

        if (e) return reject(e);

        dynamicRoutes[route.index] = true;

        resolve();
      });

    }catch(e){

      reject(e);
    }
  });
}

function __createDynamicIndex (dynamicParts, callback) {

  var _this = this;

  var indexJSON = _this.__buildIndexObj({
      index: dynamicParts.index,
      type: dynamicParts.type
    }
  );

  _this.__createIndex(dynamicParts.index, indexJSON, callback);
}

function __insert (path, setData, options, route, timestamp, modifiedOn, callback) {

  var _this = this;

  //[start:{"key":"__update", "self":"_this"}:start]

  var elasticMessage = {

    index: route.index,
    type: route.type,
    id: path,

    body: {
      created: modifiedOn,
      modified: modifiedOn,
      timestamp: timestamp,
      path: path,
      data: setData.data,
      modifiedBy:options.modifiedBy,
      createdBy:options.modifiedBy,
      _tag:setData._tag
    },

    refresh: options.refresh,
    opType: "create"
  };

  _this.__elasticCallQueue.push({method:'index', message:elasticMessage}, function (e, response) {

    //[end:{"key":"__update", "self":"_this", "error":"e"}:end]

    if (e) return callback(e);

    var inserted = elasticMessage.body;

    inserted._index = response._index;
    inserted._type = response._type;
    inserted._id = response._id;
    inserted._version = response._version;

    //inserted, inserted is because the item is definitely being created

    callback(null, inserted, inserted, true, _this.__getMeta(inserted));

  });
}

function __bulk (setData, options, callback) {

  var _this = this;

  //[start:{"key":"__bulk", "self":"_this", "error":"e"}:start]

  var bulkMessage = _this.__createBulkMessage(setData, options);

  _this.__elasticCallQueue.push({method:'bulk', message:bulkMessage}, function (e, response) {

    //[end:{"key":"__bulk", "self":"_this", "error":"e"}:end]

    if (e) return callback(e);

    //[end:{"key":"__bulk", "self":"_this", "error":"e"}:end]

    callback(null, response);
  });
}

function __createBulkMessage(setData, options){

  var _this = this;

  //[start:{"key":"__createBulkMessage", "self":"_this", "error":"e"}:start]

  var bulkMessage = {body:[]};

  var modifiedOn = Date.now();

  setData.forEach(function(bulkItem){

    bulkMessage.body.push({ index:  { _index: bulkItem.index || _this.__config.defaultIndex, _type: bulkItem.type || _this.__config.defaultType, _id: bulkItem.path } });

    bulkMessage.body.push({
      created: modifiedOn,
      modified: modifiedOn,
      timestamp: modifiedOn,
      path: bulkItem.path,
      data: bulkItem.data,
      modifiedBy:options.modifiedBy,
      createdBy:options.modifiedBy
    });
  });

  //[end:{"key":"__createBulkMessage", "self":"_this", "error":"e"}:end]

  return bulkMessage;
}

function __update (path, setData, options, route, timestamp, modifiedOn, callback) {

  var _this = this;

  //[start:{"key":"__update", "self":"_this"}:start]

  var elasticMessage = {

    "index": route.index,
    "type": route.type,
    id: path,

    body: {

      doc: {
        modified: modifiedOn,
        timestamp: timestamp,
        path: path,
        data: setData.data
      },

      upsert: {
        created: modifiedOn,
        modified: modifiedOn,
        timestamp: timestamp,
        path: path,
        data: setData.data
      }
    },
    _source: true,
    refresh: options.refresh,
    retry_on_conflict: options.retries
  };

  if (options.modifiedBy) {

    elasticMessage.body.upsert.modifiedBy = options.modifiedBy;
    elasticMessage.body.doc.modifiedBy = options.modifiedBy;
    elasticMessage.body.upsert.createdBy = options.modifiedBy;
  }

  if (setData._tag) {

    elasticMessage.body.doc._tag = setData._tag;
    elasticMessage.body.upsert._tag = setData._tag;
  }

  _this.__elasticCallQueue.push({method:'update', message:elasticMessage}, function (e, response) {

    //[end:{"key":"__update", "self":"_this", "error":"e"}:end]

    if (e) return callback(e);

    var data = response.get._source;

    var created = null;

    if (response.result == 'created') created = _this.__partialTransform(response.get, route.index, route.type);

    callback(null, data, created, true, _this.__getMeta(response.get._source));
  });
}

function remove (path, callback) {

  var _this = this;

  //[start:{"key":"remove", "self":"_this"}:start]

  var multiple = path.indexOf('*') > -1;

  var deletedCount = 0;

  var route = _this.__getRoute(path);

  var handleResponse = function (e) {

    //[end:{"key":"remove", "self":"_this", "error":"e"}:end]

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

  //we cannot delete what does not exist yet
  if (route.noIndexYet) return handleResponse(null);

  var elasticMessage = {
    index: route.index,
    type: route.type,
    refresh: true
  };

  if (multiple) {

    elasticMessage.body = {
      "query": {
        "wildcard": {
          "path": path
        }
      }
    };

    //deleteOperation = this.db.deleteByQuery.bind(this.db);

  } else elasticMessage.id = path;

  _this.count(elasticMessage, function (e, count) {

    if (e) return callback(new Error('count operation failed for delete: ' + e.toString()));

    deletedCount = count;

    var elasticArguments = {message:elasticMessage, method:'delete'};

    if (multiple) elasticArguments.method = 'deleteByQuery';

    _this.__elasticCallQueue.push(elasticArguments, handleResponse);
  });
}

function find (path, parameters, callback) {

  var _this = this;

  var searchPath = _this.preparePath(path);

  //[start:{"key":"find", "self":"_this"}:start]

  var route = _this.__getRoute(searchPath);

  //console.log('find route:::', route);

  if (route.noIndexYet) {
    //[end:{"key":"find", "self":"_this"}:end]
    return callback(null, []);
  }

  var elasticMessage = {
    "index": route.index,
    "type": route.type,
    "body": {
      "query": {
        "bool": {
          "must": []
        }
      }
    }
  };

  if (parameters.options) mongoToElastic.convertOptions(parameters.options, elasticMessage);//this is because the $not keyword works in nedb and sift, but not in elastic

  if (elasticMessage.body.from == null) elasticMessage.body.from = 0;

  if (elasticMessage.body.size == null) elasticMessage.body.size = 10000;

  var returnType = searchPath.indexOf('*'); //0,1 == array -1 == single

  if (returnType > -1) {

    //NB: elasticsearch regexes are always anchored so elastic adds a ^ at the beginning and a $ at the end.

    elasticMessage.body["query"]["bool"]["must"].push({
      "regexp": {
        "path": _this.escapeRegex(searchPath).replace(/\\\*/g, ".*")
      }
    });
  } else {
    elasticMessage.body["query"]["bool"]["must"].push({
      "terms": {
        "_id": [searchPath]
      }
    });
  }

  _this.__elasticCallQueue.push({message:elasticMessage, method:'search'}, function(e, resp){

    //[end:{"key":"find", "self":"_this"}:end]

    if (e) return callback(e);

    if (resp.hits && resp.hits.hits && resp.hits.hits.length > 0) {

      var found = resp.hits.hits;

      if (parameters.criteria)  found = _this.__filter(_this.__parseFields(parameters.criteria), found);

      callback(null, _this.__partialTransformAll(found));

    } else callback(null, []);
  });
}

function findOne (criteria, fields, callback) {

  var _this = this;

  //[start:{"key":"findOne", "self":"_this"}:start]

  var path = criteria.path;

  delete criteria.path;

  _this.find(path, {options: fields, criteria: criteria}, function (e, results) {

    //[end:{"key":"findOne", "self":"_this"}:end]

    if (e) return callback(e);

    if (results.length > 0) {

      callback(null, results[0]);//already partially transformed

    } else callback(null, null);
  })
};

function count (message, callback) {

  var _this = this;

  //[start:{"key":"count", "self":"_this"}:start]

  var countMessage = {
    index: message.index,
    type: message.type
  };

  if (message.id) countMessage.body = {
    "query": {
      "match": {
        "path": message.id
      }
    }
  };

  else if (message.body) countMessage.body = message.body;

  _this.__elasticCallQueue.push({method:'count', message:countMessage} , function (e, response) {

    //[end:{"key":"count", "self":"_this"}:end]

    if (e) return callback(e);

    callback(null, response.count);
  });
}



function __partialTransformAll (dataItems) {

  var _this = this;

  return dataItems.map(function (dataItem) {
    return _this.__partialTransform(dataItem);
  })
}

function __partialTransform (dataItem, index, type) {

  return {
    _id: dataItem._id ? dataItem._id : dataItem._source.path,
    _index: dataItem._index ? dataItem._index : index,
    _type: dataItem._type ? dataItem._type : type,
    _score: dataItem._score,
    _version: dataItem._version,
    _tag: dataItem._source._tag,
    created: dataItem._source.created,
    deleted: dataItem._source.deleted,
    modified: dataItem._source.modified,
    createdBy: dataItem._source.createdBy,
    modifiedBy: dataItem._source.modifiedBy,
    deletedBy: dataItem._source.deletedBy,
    data: dataItem._source.data
  };
}

function __partialInsertTransform (createdObj, response) {

  return {
    _id: response._id,
    _index: response._index,
    _type: response._type,
    _version: response._version,
    _tag: createdObj._tag,
    created: createdObj.created,
    deleted: createdObj.deleted,
    modified: createdObj.modified,
    createdBy: createdObj.createdBy,
    modifiedBy: createdObj.modifiedBy,
    deletedBy: createdObj.deletedBy,
    data: createdObj.data
  };
}

function transform (dataObj, meta) {

  //[start:{"key":"transform", "self":"this"}:start]

  var transformed = {data: dataObj.data};

  if (!meta) {

    meta = {};

    if (dataObj.created) meta.created = dataObj.created;

    if (dataObj.modified) meta.modified = dataObj.modified;

    if (dataObj.modifiedBy) meta.modifiedBy = dataObj.modifiedBy;

    if (dataObj.createdBy) meta.createdBy = dataObj.createdBy;
  }

  transformed._meta = meta;

  if (!dataObj._id) dataObj._id = dataObj.path;

  transformed._meta.path = dataObj._id;
  transformed._meta._id = dataObj._id;

  if (dataObj._tag) transformed._meta.tag = dataObj._tag;

  //[end:{"key":"transform", "self":"this"}:end]

  return transformed;
}

function transformAll (items) {

  var _this = this;

  return items.map(function (item) {

    return _this.transform(item, null);
  })
}

function __parseFields (fields) {

  //[start:{"key":"__parseFields", "self":"this"}:start]

  traverse(fields).forEach(function (value) {

    if (value) {

      var _thisNode = this;

      //ignore elements in arrays
      if (_thisNode.parent && Array.isArray(_thisNode.parent.node)) return;

      if (typeof _thisNode.key == 'string') {

        //ignore directives
        if (_thisNode.key.indexOf('$') == 0) return;

        if (_thisNode.key == '_id') {
          _thisNode.parent.node['_source._id'] = value;
          return _thisNode.remove();
        }

        if (_thisNode.key == 'path' || _thisNode.key == "_meta.path") {
          _thisNode.parent.node['_source.path'] = value;
          return _thisNode.remove();
        }

        if (_thisNode.key == 'created' || _thisNode.key == "_meta.created") {
          _thisNode.parent.node['_source.created'] = value;
          return _thisNode.remove();
        }

        if (_thisNode.key == 'modified' || _thisNode.key == "_meta.modified") {
          _thisNode.parent.node['_source.modified'] = value;
          return _thisNode.remove();
        }

        if (_thisNode.key == 'timestamp' || _thisNode.key == "_meta.timestamp") {
          _thisNode.parent.node['_source.timestamp'] = value;
          return _thisNode.remove();
        }

        var propertyKey = _thisNode.key;

        if (propertyKey.indexOf('data.') == 0) _thisNode.parent.node['_source.' + propertyKey] = value;
        else if (propertyKey.indexOf('_data.') == 0) _thisNode.parent.node['_source.' + propertyKey] = value;
        //prepend with data.
        else _thisNode.parent.node['_source.data.' + propertyKey] = value;

        return _thisNode.remove();
      }
    }
  });

      //[end:{"key":"__parseFields", "self":"this"}:end]

  return fields;
}

function __getMeta (response) {

  var meta = {
    created: response.created,
    modified: response.modified,
    modifiedBy: response.modifiedBy,
    timestamp: response.timestamp,
    path: response.path,
    _id: response.path
  };

  return meta;
}

function __wildcardMatch (pattern, matchTo) {

  return this.__comedian.matches(pattern, matchTo);
}

function escapeRegex (str) {

  if (typeof str !== 'string') throw new TypeError('Expected a string');

  return str.replace(/[|\\{}()[\]^$+*?.]/g, "\\$&");
}

function preparePath (path) {

  //strips out duplicate sequential wildcards, ie simon***bishop -> simon*bishop

  //[start:{"key":"preparePath", "self":"this"}:start]

  if (!path) return '*';

  var prepared = '';

  var lastChar = null;

  for (var i = 0; i < path.length; i++) {

    if (path[i] == '*' && lastChar == '*') continue;

    prepared += path[i];

    lastChar = path[i];
  }

  //[end:{"key":"preparePath", "self":"this"}:end]

  return prepared;
}

function __filter (criteria, items) {
  try {
    return sift(criteria, items);
  } catch (e) {
    throw new Error('filter failed: ' + e.toString(), e);
  }
}

function setUpCache () {

  var _this = this;

  var cache = new Cache(_this.__config.cache);

  Object.defineProperty(this, 'cache', {value: cache});

  _this.__oldFind = _this.find;

  _this.find = function (path, parameters, callback) {

    if (path.indexOf && path.indexOf('*') == -1) {

      return _this.cache.get(path, function (e, item) {

        if (e) return callback(e);

        if (item) return callback(null, [item]);

        _this.__oldFind(path, parameters, function (e, items) {

          if (e) return callback(e);

          if (!items || items.length == 0) return callback(null, []);

          _this.cache.set(path, items[0], function (e) {

            return callback(e, items);
          });
        });
      });
    }

    return this.__oldFind(path, parameters, callback);

  }.bind(this);

  this.__oldFindOne = this.findOne;

  _this.findOne = function (criteria, fields, callback) {

    if (criteria.path && criteria.path.indexOf('*') > -1) return this.__oldFindOne(criteria, fields, callback);

    _this.cache.get(criteria.path, function (e, item) {

      if (e) return callback(e);

      if (item) return callback(null, item);

      _this.__oldFindOne(criteria, fields, function (e, item) {

        if (e) return callback(e);

        if (!item) return callback(null, null);

        return callback(e, item);
      });
    });
  }.bind(this);

  this.__oldRemove = this.remove;

  this.remove = function (path, callback) {

    if (path.indexOf && path.indexOf('*') == -1) {

      //its ok if the actual remove fails, as the cache will refresh
      _this.cache.remove(path, function (e) {

        if (e) return callback(e);

        _this.__oldRemove(path, callback);
      });

    } else {

      _this.find(path, {fields: {path: 1}}, null, function (e, items) {

        if (e) return callback(e);

        //clear the items from the cache
        async.eachSeries(items, function (item, itemCB) {

          _this.cache.remove(item.path, itemCB);

        }, function (e) {

          if (e) return callback(e);

          _this.__oldRemove(path, callback);
        });
      });
    }
  }.bind(this);

  this.__oldUpdate = this.update;

  _this.update = function (criteria, data, options, callback) {

    _this.__oldUpdate(criteria, data, options, function (e, response) {

      if (e) return callback(e);

      _this.cache.set(criteria.path, data, function (e) {

        if (e) return callback(e);

        return callback(null, response);

      });
    });
  }.bind(this);

  console.warn('data caching is on, be sure you have redis up.');
}

function __matchRoute (path, pattern) {

  if (this.__wildcardMatch(pattern, path)) return true;

  var baseTagPath = '/_TAGS';

  if (path.substring(0, 1) != '/') baseTagPath += '/';

  return this.__wildcardMatch(baseTagPath + pattern, path);
}

function __getRoute (path, callback) {

  var _this = this;

  //[start:{"key":"__getRoute", "self":"_this"}:start]

  var route = null;

  _this.__config.dataroutes.every(function (dataStoreRoute) {

    var pattern = dataStoreRoute.pattern;

    if (dataStoreRoute.dynamic) pattern = dataStoreRoute.pattern.split('{')[0] + '*';

    if (_this.__matchRoute(path, pattern)) route = dataStoreRoute;

    return route == null;
  });

  if (!route) {
    //[end:{"key":"__getRoute", "self":"_this"}:end]
    throw new Error('route for path ' + path + ' does not exist');
  }

  if (route.dynamic){

    //[end:{"key":"__getRoute", "self":"_this"}:end]
    return  _this.__getDynamicParts(route, path);
  }

  //[end:{"key":"__getRoute", "self":"_this"}:end]
  return route;
}


function __getDynamicParts (dataStoreRoute, path) {

  var _this = this;

  //[start:{"key":"__prepareDynamicIndex", "self":"_this"}:start]

  var dynamicParts = {dynamic:true};

  var pathSegments = path.split('/');

  if (!dataStoreRoute.pathLocations){

    var locations = {};

    dataStoreRoute.pattern.split('/').every(function(segment, segmentIndex){

      if (segment == '{index}') locations.index = segmentIndex;

      if (segment == '{type}') locations.type = segmentIndex;

      return !(locations.index && locations.type);
    });

    dataStoreRoute.pathLocations = locations;
  }

  if (dataStoreRoute.pathLocations.index) dynamicParts.index = pathSegments[dataStoreRoute.pathLocations.index];

  if (dataStoreRoute.pathLocations.type) dynamicParts.type = pathSegments[dataStoreRoute.pathLocations.type];

  if (!dynamicParts.index) dynamicParts.index = dataStoreRoute.index;

  if (!dynamicParts.type) dynamicParts.type = dataStoreRoute.type;

  //[end:{"key":"__prepareDynamicIndex", "self":"_this"}:end]

  return dynamicParts;
}

function __createIndex (index, indexConfig, callback) {

  var _this = this;

   //[start:{"key":"__createIndex", "self":"_this"}:start]

  _this.__elasticAdminCallQueue.push({method:'indices.create', message:indexConfig}, function (e) {

    if (e && e.toString().indexOf('[index_already_exists_exception]') == -1) {

      //[end:{"key":"__createIndex", "self":"_this"}:end]
      return callback(new Error('failed creating index ' + index + ':' + e.toString(), e));
    }

    callback();
  });
}

function __buildIndexObj (indexConfig) {

  var _this = this;

  //[start:{"key":"__buildIndexObj", "self":"_this"}:start]

  if (indexConfig.index == null) indexConfig.index = _this.__config.defaultIndex;

  if (indexConfig.type == null) indexConfig.type = _this.__config.defaultType;

  var indexJSON = {
    index: indexConfig.index,
    body: {
      "mappings": {}
    }
  };

  var typeJSON = {
    "properties": {
      "path": {"type": "keyword"},
      "data": {"type": "object"},
      "created": {"type": "date"},
      "timestamp": {"type": "date"},
      "modified": {"type": "date"},
      "modifiedBy": {"type": "keyword"},
      "createdBy": {"type": "keyword"}
    }
  };

  indexJSON.body.mappings[indexConfig.type] = typeJSON;

  //add any additional mappings
  if (indexConfig.body && indexConfig.body.mappings && indexConfig.body.mappings[indexConfig.type])

    Object.keys(indexConfig.body.mappings[indexConfig.type].properties).forEach(function (fieldName) {

      var mappingFieldName = fieldName;

      if (indexJSON.body.mappings[indexConfig.type].properties[mappingFieldName] == null) {
        indexJSON.body.mappings[indexConfig.type].properties[mappingFieldName] = indexConfig.body.mappings[indexConfig.type].properties[fieldName];
      }
    });

  //[end:{"key":"__buildIndexObj", "self":"_this"}:end]

  return indexJSON;
}

function __createIndexes (callback) {

  var _this = this;

  if (!_this.__config.indexes) _this.__config.indexes = [];

  var defaultIndexFound = false;

  _this.__config.indexes.forEach(function (indexConfig) {

    if (indexConfig.index === _this.__config.defaultIndex) defaultIndexFound = true;
  });

  var indexJSON = _this.__buildIndexObj({
      index: _this.__config.defaultIndex,
      type: _this.__config.defaultType
    }
  );

  if (!defaultIndexFound) {
    _this.__config.indexes.push(indexJSON);
  }

  async.eachSeries(_this.__config.indexes, function (index, indexCB) {

    if (index.index != _this.defaultIndex) indexJSON = _this.__buildIndexObj(index);

    _this.__createIndex(index.index, indexJSON, indexCB);

  }, function (e) {

    if (e) return callback(e);

    if (!_this.__config.dataroutes) _this.__config.dataroutes = [];

    //last route goes to default index

    var defaultRouteFound = false;

    _this.__config.dataroutes.forEach(function (route) {
      if (route.pattern == '*') defaultRouteFound = true;
    });

    if (!defaultRouteFound) _this.__config.dataroutes.push({pattern: '*', index: _this.__config.defaultIndex});

    callback();
  });
}


function __executeElasticAdminMessage (elasticCall, callback){

  try{

    if (elasticCall.method == 'indices.create')
      return this.db.indices.create(elasticCall.message, callback);

    callback(new Error('unknown admin message: ' + elasticCall.method));

  }catch(e){

    callback(e);
  }
}

function __executeElasticMessage(elasticCall, callback){

  try{

    this.db[elasticCall.method].call(this.db, elasticCall.message, callback);

  }catch(e){

    callback(e);
  }
}


/* interface stubs */


ElasticProvider.prototype.startCompacting = function (interval, callback, compactionHandler) {


  return callback();
};

ElasticProvider.prototype.stopCompacting = function (callback) {


  return callback();
};

ElasticProvider.prototype.compact = function (callback) {


  return callback();
};



module.exports = ElasticProvider;
