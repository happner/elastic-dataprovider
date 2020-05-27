const mongoToElastic = require('./lib/mongo-to-elastic');
const async = require('async');
const traverse = require('traverse');
const Comedian = require('co-median');
const Cache = require('redis-lru-cache');
const hyperid = require('happner-hyperid').create({urlSafe: true});
const micromustache = require('micromustache');


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
  upsert: 0,
  update: 1,
  insert: 2,
  bulk: 3,
};


/* initialize and stop */

ElasticProvider.prototype.initialize = initialize;

ElasticProvider.prototype.__initializeRoutes = __initializeRoutes;

ElasticProvider.prototype.stop = stop;


/* upsert, insert, update and remove */

ElasticProvider.prototype.upsert = upsert;

ElasticProvider.prototype.__ensureDynamic = __ensureDynamic;

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


ElasticProvider.prototype.__executeElasticMessage = __executeElasticMessage;

ElasticProvider.prototype.__pushElasticMessage = __pushElasticMessage;

/* utility methods */

ElasticProvider.prototype.__wildcardMatch = __wildcardMatch;

ElasticProvider.prototype.escapeRegex = mongoToElastic.escapeRegex;

ElasticProvider.prototype.preparePath = preparePath;


function initialize(callback) {
  const _this = this;

  const elasticsearch = require('elasticsearch');

  try {
    _this.__initializeRoutes();

    // yada yada yada: https://github.com/elastic/elasticsearch-js/issues/196
    const AgentKeepAlive = require('agentkeepalive');

    _this.__config.createNodeAgent = function(connection, config) {
      return new AgentKeepAlive(connection.makeAgentConfig(config));
    };

    const client = new elasticsearch.Client(_this.__config);

    client.ping({
      requestTimeout: 30000,
    }, function(e) {
      if (e) return callback(e);

      Object.defineProperty(_this, 'db', {value: client});

      Object.defineProperty(_this, '__elasticCallQueue', {value: async.queue(_this.__executeElasticMessage.bind(_this), _this.__config.elasticCallConcurrency)});

      if (_this.__config.cache) _this.setUpCache();

      _this.__createIndexes(callback);
    });
  } catch (e) {
    callback(e);
  }
}

function stop(callback) {
  this.db.close();

  callback();
}


function __initializeRoutes() {
  const _this = this;

  _this.__config.dataroutes.forEach(function(route) {
    if (!route.index) route.index = _this.__config.defaultIndex;
    if (!route.type) route.type = _this.__config.defaultType;
  });
}

function upsert(path, setData, options, dataWasMerged, callback) {
  const _this = this;

  // [start:{"key":"upsert", "self":"_this"}:start]

  try {
    if (!options) options = {};

    options.refresh = options.refresh === false || options.refresh === 'false'?'false':'true'; // true is slow but reliable

    if (options.upsertType == _this.UPSERT_TYPE.bulk)// dynamic index is generated automatically using "index" in bulk inserts
    {
      return _this.__bulk(path, setData, options, callback);
    }

    const modifiedOn = Date.now();

    const timestamp = setData.data.timestamp ? setData.data.timestamp : modifiedOn;

    if (options.upsertType == null) options.upsertType = _this.UPSERT_TYPE.upsert;

    const route = _this.__getRoute(path, setData.data);

    _this.__ensureDynamic(route)// upserting so we need to make sure our index exists

        .then(function() {
          if (options.retries == null) {
            options.retries = _this.__config.elasticCallConcurrency + 20;
          }// retry_on_conflict: same size as the max amount of concurrent calls to elastic and some.

          if (options.upsertType == _this.UPSERT_TYPE.insert) {
            return _this.__insert(path, setData, options, route, timestamp, modifiedOn, callback);
          }

          // [end:{"key":"upsert", "self":"_this"}:end]
          _this.__update(path, setData, options, route, timestamp, modifiedOn, callback);
        })

        .catch(callback);
  } catch (e) {
    callback(e);
  }
}

function __ensureDynamic(route) {
  const _this = this;

  return new Promise(function(resolve, reject) {
    if (!route.dynamic || _this.__dynamicRoutes[route.index]) return resolve();

    _this.__createIndex(_this.__buildIndexObj(route)).then(function() {
      _this.__dynamicRoutes[route.index] = true;

      resolve();
    }).catch(reject);
  });
}

function __insert(path, setData, options, route, timestamp, modifiedOn, callback) {
  const _this = this;

  // [start:{"key":"__update", "self":"_this"}:start]

  const elasticMessage = {

    index: route.index,
    type: route.type,
    id: path,

    body: {
      created: modifiedOn,
      modified: modifiedOn,
      timestamp: timestamp,
      path: path,
      data: setData.data,
      modifiedBy: options.modifiedBy,
      createdBy: options.modifiedBy,
      _tag: setData._tag,
    },

    refresh: options.refresh,
    opType: 'create',
  };

  _this.__pushElasticMessage('index', elasticMessage)

      .then(function(response) {
        const inserted = elasticMessage.body;

        inserted._index = response._index;
        inserted._type = response._type;
        inserted._id = response._id;
        inserted._version = response._version;

        // inserted, inserted is because the item is definitely being created

        callback(null, inserted, inserted, false, _this.__getMeta(inserted));
      })

      .catch(callback);
}

function __bulk(path, setData, options, callback) {
  const _this = this;

  // [start:{"key":"__bulk", "self":"_this", "error":"e"}:start]

  _this.__createBulkMessage(path, setData, options)

      .then(function(bulkMessage) {
        return _this.__pushElasticMessage('bulk', bulkMessage);
      })

      .then(function(response) {
        callback(null, response, null, true, response);
      })

      .catch(callback);
}

function __createBulkMessage(path, setData, options) {
  const _this = this;

  // [start:{"key":"__createBulkMessage", "self":"_this", "error":"e"}:start]

  return new Promise(function(resolve, reject) {
    let bulkData = setData;

    // coming in from happn, not an object but a raw [] so assigned to data.value
    if (setData.data && setData.data.value) bulkData = setData.data.value;

    if (bulkData.length > 1000) throw new Error('bulk batches can only be 1000 entries or less');

    const bulkMessage = {body: [], refresh: options.refresh, _source: true};

    const modifiedOn = Date.now();

    async.eachSeries(bulkData, function(bulkItem, bulkItemCB) {
      let route;

      if (bulkItem.path) route = _this.__getRoute(bulkItem.path, bulkItem.data);

      else route = _this.__getRoute(path, bulkItem.data);

      _this.__ensureDynamic(route)// upserting so we need to make sure our index exists

          .then(function() {
            bulkMessage.body.push({
              index:
            {
              _index: route.index,
              _type: route.type,
              _id: route.path,
            },
            });

            bulkMessage.body.push({
              created: modifiedOn,
              modified: modifiedOn,
              timestamp: bulkItem.data.timestamp || modifiedOn,
              path: route.path,
              data: bulkItem.data,
              modifiedBy: options.modifiedBy,
              createdBy: options.modifiedBy,
            });

            bulkItemCB();
          })

          .catch(bulkItemCB);
    }, function(e) {
      // [end:{"key":"__createBulkMessage", "self":"_this", "error":"e"}:end]

      if (e) return reject(e);

      resolve(bulkMessage);
    });
  });
}

function __update(path, setData, options, route, timestamp, modifiedOn, callback) {
  const _this = this;

  // [start:{"key":"__update", "self":"_this"}:start]

  const elasticMessage = {

    'index': route.index,
    'type': route.type,
    'id': path,

    'body': {

      doc: {
        modified: modifiedOn,
        timestamp: timestamp,
        path: path,
        data: setData.data,
      },

      upsert: {
        created: modifiedOn,
        modified: modifiedOn,
        timestamp: timestamp,
        path: path,
        data: setData.data,
      },
    },
    '_source': true,
    'refresh': options.refresh,
    'retryOnConflict': options.retries,
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

  _this.__pushElasticMessage('update', elasticMessage)

      .then(function(response) {
        const data = response.get._source;

        let created = null;

        if (response.result == 'created') created = _this.__partialTransform(response.get, route.index, route.type);

        callback(null, data, created, true, _this.__getMeta(response.get._source));
      })

      .catch(callback);
}

function remove(path, callback) {
  const _this = this;

  // [start:{"key":"remove", "self":"_this"}:start]

  const multiple = path.indexOf('*') > -1;

  let deletedCount = 0;

  const route = _this.__getRoute(path);

  const handleResponse = function(e) {
    // [end:{"key":"remove", "self":"_this", "error":"e"}:end]

    if (e) return callback(e);

    const deleteResponse = {
      'data': {
        'removed': deletedCount,
      },
      '_meta': {
        'timestamp': Date.now(),
        'path': path,
      },
    };

    callback(null, deleteResponse);
  };

  // we cannot delete what does not exist yet
  if (route.noIndexYet) return handleResponse(null);

  const elasticMessage = {
    index: route.index,
    type: route.type,
    refresh: true,
  };

  if (multiple) {
    elasticMessage.body = {
      'query': {
        'wildcard': {
          'path': path,
        },
      },
    };

    // deleteOperation = this.db.deleteByQuery.bind(this.db);
  } else elasticMessage.id = path;

  _this.count(elasticMessage, function(e, count) {
    if (e) return callback(new Error('count operation failed for delete: ' + e.toString()));

    deletedCount = count;

    let method = 'delete';

    if (multiple) method = 'deleteByQuery';

    _this.__pushElasticMessage(method, elasticMessage)

        .then(function(response) {
          handleResponse(null, response);
        })

        .catch(handleResponse);
  });
}


function find(path,parameters, callback)
{
  const _this = this;

  const searchPath = _this.preparePath(path);

  // [start:{"key":"find", "self":"_this"}:start]

  const route = _this.__getRoute(searchPath);

  if (route.noIndexYet) {
    // [end:{"key":"find", "self":"_this"}:end]
    return callback(null, []);
  }
  if(!parameters.criteria)
    parameters.criteria = {};
  if(!parameters.criteria.path)
    parameters.criteria.path = route.path;
  let searchString = "";
  try {
     searchString = mongoToElastic.convertCriteria(parameters.criteria)
  } catch(e)
  {
    callback(e)
  }
    const query = {
      'query': {
        'constant_score': {
          'filter': {
            "query_string": {
              "query":   searchString ,
            }
          }
        }
      }
    };




  const elasticMessage = {
    'index': route.index,
    'type': route.type,
    'body':query
  };



  if (parameters.options) mongoToElastic.convertOptions(parameters.options, elasticMessage);// this is because the $not keyword works in nedb and sift, but not in elastic

  if (elasticMessage.body.from == null) elasticMessage.body.from = 0;

  if (elasticMessage.body.size == null) elasticMessage.body.size = 10000;



  _this.__pushElasticMessage('search', elasticMessage)

      .then(function(resp) {
        if (resp.hits && resp.hits.hits && resp.hits.hits.length > 0) {
          let found = resp.hits.hits;

          callback(null, _this.__partialTransformAll(found));
        } else callback(null, []);
      })
      .catch(callback);
}


function findOne(criteria, fields, callback) {
  const _this = this;

  // [start:{"key":"findOne", "self":"_this"}:start]

  const path = criteria.path;

  delete criteria.path;

  _this.find(path, {options: fields, criteria: criteria}, function(e, results) {
    // [end:{"key":"findOne", "self":"_this"}:end]

    if (e) return callback(e);

    if (results.length > 0) {
      callback(null, results[0]);// already partially transformed
    } else callback(null, null);
  });
};

function count(pathOrMessage, parametersOrCallBack, callback) {
  const _this = this;
  let countMessage = {};

  const path = '';
  if (typeof pathOrMessage === 'string') {
    const searchPath = _this.preparePath(pathOrMessage);
    const route = _this.__getRoute(searchPath);
    countMessage = {
      'index': route.index,
      'type': route.type,
      'body': {'query': {}},
    };
    countMessage;

    if (searchPath.indexOf('*') > -1) {
      countMessage.body.query = {
        'regexp': {
          'path': _this.escapeRegex(searchPath).replace(/\\\*/g, '.*'),
        },
      };
    } else {
      countMessage.body.query = {
        'match': {
          'path': searchPath,
        },
      };
    }
  } else {
    countMessage = {
      index: pathOrMessage.index,
      type: pathOrMessage.type,
    };
    if (pathOrMessage.id) {
      countMessage.body = {
        'query': {
          'match': {
            'path': pathOrMessage.id,
          },
        },
      };
    } else if (pathOrMessage.body) countMessage.body = pathOrMessage.body;
  }
  if (typeof parametersOrCallBack === 'function') {
    callback = parametersOrCallBack;
    parametersOrCallBack = {};
  } else
  if (parametersOrCallBack.options) mongoToElastic.convertOptions(parametersOrCallBack.options, elasticMessage);

  _this.__pushElasticMessage('count', countMessage)

      .then(function(response) {
        callback(null, response.count);
      })
      .catch(callback);
}



function __partialTransformAll(dataItems) {
  const _this = this;

  return dataItems.map(function(dataItem) {
    return _this.__partialTransform(dataItem);
  });
}

function __partialTransform(dataItem, index, type) {
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
    data: dataItem._source.data,
    timestamp: dataItem._source.timestamp,
  };
}

function __partialInsertTransform(createdObj, response) {
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
    data: createdObj.data,
  };
}

function transform(dataObj, meta) {
  // [start:{"key":"transform", "self":"this"}:start]

  const transformed = {data: dataObj.data};

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

  // [end:{"key":"transform", "self":"this"}:end]

  return transformed;
}

function transformAll(items) {
  const _this = this;

  return items.map(function(item) {
    return _this.transform(item, null);
  });
}

function __parseFields(fields) {
  // [start:{"key":"__parseFields", "self":"this"}:start]

  traverse(fields).forEach(function(value) {
    if (value) {
      const _thisNode = this;

      // ignore elements in arrays
      if (_thisNode.parent && Array.isArray(_thisNode.parent.node)) return;

      if (typeof _thisNode.key == 'string') {
        // ignore directives
        if (_thisNode.key.indexOf('$') == 0) return;

        if (_thisNode.key == '_id') {
          _thisNode.parent.node['_source._id'] = value;
          return _thisNode.remove();
        }

        if (_thisNode.key == 'path' || _thisNode.key == '_meta.path') {
          _thisNode.parent.node['_source.path'] = value;
          return _thisNode.remove();
        }

        if (_thisNode.key == 'created' || _thisNode.key == '_meta.created') {
          _thisNode.parent.node['_source.created'] = value;
          return _thisNode.remove();
        }

        if (_thisNode.key == 'modified' || _thisNode.key == '_meta.modified') {
          _thisNode.parent.node['_source.modified'] = value;
          return _thisNode.remove();
        }

        if (_thisNode.key == 'timestamp' || _thisNode.key == '_meta.timestamp') {
          _thisNode.parent.node['_source.timestamp'] = value;
          return _thisNode.remove();
        }

        const propertyKey = _thisNode.key;

        if (propertyKey.indexOf('data.') == 0) _thisNode.parent.node['_source.' + propertyKey] = value;
        else if (propertyKey.indexOf('_data.') == 0) _thisNode.parent.node['_source.' + propertyKey] = value;
        // prepend with data.
        else _thisNode.parent.node['_source.data.' + propertyKey] = value;

        return _thisNode.remove();
      }
    }
  });

  // [end:{"key":"__parseFields", "self":"this"}:end]

  return fields;
}

function __getMeta(response) {
  const meta = {
    created: response.created,
    modified: response.modified,
    modifiedBy: response.modifiedBy,
    timestamp: response.timestamp,
    path: response.path,
    _id: response.path,
  };

  return meta;
}

function __wildcardMatch(pattern, matchTo) {
  return this.__comedian.matches(pattern, matchTo);
}

function preparePath(path) {
  // strips out duplicate sequential wildcards, ie simon***bishop -> simon*bishop

  // [start:{"key":"preparePath", "self":"this"}:start]

  if (!path) return '*';

  let prepared = '';

  let lastChar = null;

  for (let i = 0; i < path.length; i++) {
    if (path[i] == '*' && lastChar == '*') continue;

    prepared += path[i];

    lastChar = path[i];
  }

  // [end:{"key":"preparePath", "self":"this"}:end]

  return prepared;
}

function __filter(criteria, items) {
  try {
    return sift(criteria, items);
  } catch (e) {
    throw new Error('filter failed: ' + e.toString(), e);
  }
}

function setUpCache() {
  const _this = this;

  const cache = new Cache(_this.__config.cache);

  Object.defineProperty(this, 'cache', {value: cache});

  _this.__oldFind = _this.find;

  _this.find = function(path, parameters, callback) {
    if (path.indexOf && path.indexOf('*') == -1) {
      return _this.cache.get(path, function(e, item) {
        if (e) return callback(e);

        if (item) return callback(null, [item]);

        _this.__oldFind(path, parameters, function(e, items) {
          if (e) return callback(e);

          if (!items || items.length == 0) return callback(null, []);

          _this.cache.set(path, items[0], function(e) {
            return callback(e, items);
          });
        });
      });
    }

    return this.__oldFind(path, parameters, callback);
  }.bind(this);

  this.__oldFindOne = this.findOne;

  _this.findOne = function(criteria, fields, callback) {
    if (criteria.path && criteria.path.indexOf('*') > -1) return this.__oldFindOne(criteria, fields, callback);

    _this.cache.get(criteria.path, function(e, item) {
      if (e) return callback(e);

      if (item) return callback(null, item);

      _this.__oldFindOne(criteria, fields, function(e, item) {
        if (e) return callback(e);

        if (!item) return callback(null, null);

        return callback(e, item);
      });
    });
  }.bind(this);

  this.__oldRemove = this.remove;

  this.remove = function(path, callback) {
    if (path.indexOf && path.indexOf('*') == -1) {
      // its ok if the actual remove fails, as the cache will refresh
      _this.cache.remove(path, function(e) {
        if (e) return callback(e);

        _this.__oldRemove(path, callback);
      });
    } else {
      _this.find(path, {fields: {path: 1}}, null, function(e, items) {
        if (e) return callback(e);

        // clear the items from the cache
        async.eachSeries(items, function(item, itemCB) {
          _this.cache.remove(item.path, itemCB);
        }, function(e) {
          if (e) return callback(e);

          _this.__oldRemove(path, callback);
        });
      });
    }
  };

  this.__oldUpdate = this.update;

  _this.update = function(criteria, data, options, callback) {
    _this.__oldUpdate(criteria, data, options, function(e, response) {
      if (e) return callback(e);

      _this.cache.set(criteria.path, data, function(e) {
        if (e) return callback(e);

        return callback(null, response);
      });
    });
  };

  console.warn('data caching is on, be sure you have redis up.');
}

function __matchRoute(path, pattern) {
  if (this.__wildcardMatch(pattern, path)) return true;

  let baseTagPath = '/_TAGS';

  if (path.substring(0, 1) != '/') baseTagPath += '/';

  return this.__wildcardMatch(baseTagPath + pattern, path);
}

function __getRoute(path, obj) {
  const _this = this;

  // [start:{"key":"__getRoute", "self":"_this"}:start]

  let route = null;

  let routePath = path.toString();

  if (obj) routePath = micromustache.render(routePath, obj);

  if (routePath.indexOf('{id}') > -1) routePath = routePath.replace('{id}', hyperid());

  _this.__config.dataroutes.every(function(dataStoreRoute) {
    let pattern = dataStoreRoute.pattern;

    if (dataStoreRoute.dynamic) pattern = dataStoreRoute.pattern.split('{')[0] + '*';

    if (_this.__matchRoute(routePath, pattern)) route = dataStoreRoute;

    return route == null;
  });

  if (!route) {
    // [end:{"key":"__getRoute", "self":"_this"}:end]
    throw new Error('route for path ' + routePath + ' does not exist');
  }

  if (route.dynamic) {
    // [end:{"key":"__getRoute", "self":"_this"}:end]
    route = _this.__getDynamicParts(route, routePath);
  }

  // [end:{"key":"__getRoute", "self":"_this"}:end]

  route.path = routePath;

  return route;
}


function __getDynamicParts(dataStoreRoute, path) {
  const _this = this;

  // [start:{"key":"__prepareDynamicIndex", "self":"_this"}:start]

  const dynamicParts = {dynamic: true};

  const pathSegments = path.split('/');

  if (!dataStoreRoute.pathLocations) {
    const locations = {};

    dataStoreRoute.pattern.split('/').every(function(segment, segmentIndex) {
      if (segment == '{{index}}') locations.index = segmentIndex;

      if (segment == '{{type}}') locations.type = segmentIndex;

      return !(locations.index && locations.type);
    });

    dataStoreRoute.pathLocations = locations;
  }

  if (dataStoreRoute.pathLocations.index) dynamicParts.index = pathSegments[dataStoreRoute.pathLocations.index];

  if (dataStoreRoute.pathLocations.type) dynamicParts.type = pathSegments[dataStoreRoute.pathLocations.type];

  if (!dynamicParts.index) dynamicParts.index = dataStoreRoute.index;

  if (!dynamicParts.type) dynamicParts.type = dataStoreRoute.type;

  // [end:{"key":"__prepareDynamicIndex", "self":"_this"}:end]

  return dynamicParts;
}

function __createIndex(indexConfig) {
  const _this = this;

  // [start:{"key":"__createIndex", "self":"_this"}:start]

  return new Promise(function(resolve, reject) {
    _this.__pushElasticMessage('indices.create', indexConfig)

        .then(function() {
        // [end:{"key":"__createIndex", "self":"_this"}:end]

          resolve();
        })

        .catch(function(e) {
        // [end:{"key":"__createIndex", "self":"_this", "error":"e"}:end]

          if (e && e.toString().indexOf('[index_already_exists_exception]') == -1) {
            return reject(new Error('failed creating index ' + indexConfig.index + ':' + e.toString(), e));
          }

          resolve();
        });
  });
}

function __buildIndexObj(indexConfig) {
  const _this = this;

  // [start:{"key":"__buildIndexObj", "self":"_this"}:start]

  if (indexConfig.index == null) indexConfig.index = _this.__config.defaultIndex;

  if (indexConfig.type == null) indexConfig.type = _this.__config.defaultType;

  const indexJSON = {
    index: indexConfig.index,
    body: {
      'mappings': {},
    },
  };

  const typeJSON = {
    'properties': {
      'path': {'type': 'keyword'},
      'data': {'type': 'object'},
      'created': {'type': 'date'},
      'timestamp': {'type': 'date'},
      'modified': {'type': 'date'},
      'modifiedBy': {'type': 'keyword'},
      'createdBy': {'type': 'keyword'},
    },
  };

  indexJSON.body.mappings[indexConfig.type] = typeJSON;

  // add any additional mappings
  if (indexConfig.body && indexConfig.body.mappings && indexConfig.body.mappings[indexConfig.type]) {
    Object.keys(indexConfig.body.mappings[indexConfig.type].properties).forEach(function(fieldName) {
      const mappingFieldName = fieldName;

      if (indexJSON.body.mappings[indexConfig.type].properties[mappingFieldName] == null) {
        indexJSON.body.mappings[indexConfig.type].properties[mappingFieldName] = indexConfig.body.mappings[indexConfig.type].properties[fieldName];
      }
    });
  }

  // [end:{"key":"__buildIndexObj", "self":"_this"}:end]

  return indexJSON;
}

function __createIndexes(callback) {
  const _this = this;

  if (!_this.__config.indexes) _this.__config.indexes = [];

  let defaultIndexFound = false;

  _this.__config.indexes.forEach(function(indexConfig) {
    if (indexConfig.index === _this.__config.defaultIndex) defaultIndexFound = true;
  });

  let indexJSON = _this.__buildIndexObj({
    index: _this.__config.defaultIndex,
    type: _this.__config.defaultType,
  },
  );

  if (!defaultIndexFound) {
    _this.__config.indexes.push(indexJSON);
  }

  async.eachSeries(_this.__config.indexes, function(index, indexCB) {
    if (index.index != _this.defaultIndex) indexJSON = _this.__buildIndexObj(index);

    _this.__createIndex(indexJSON).then(indexCB).catch(indexCB);
  }, function(e) {
    if (e) return callback(e);

    if (!_this.__config.dataroutes) _this.__config.dataroutes = [];

    // last route goes to default index

    let defaultRouteFound = false;

    _this.__config.dataroutes.forEach(function(route) {
      if (route.pattern == '*') defaultRouteFound = true;
    });

    if (!defaultRouteFound) _this.__config.dataroutes.push({pattern: '*', index: _this.__config.defaultIndex});

    callback();
  });
}

function __pushElasticMessage(method, message) {
  const _this = this;

  return new Promise(function(resolve, reject) {
    _this.__elasticCallQueue.push({method: method, message: message}, function(e, response) {
      if (e) return reject(e);

      resolve(response);
    });
  });
}

function __executeElasticMessage(elasticCall, callback) {
  try {
    if (elasticCall.method == 'indices.create') return this.db.indices.create(elasticCall.message, callback);

    this.db[elasticCall.method].call(this.db, elasticCall.message, callback);
  } catch (e) {
    callback(e);
  }
}

/* interface stubs */


ElasticProvider.prototype.startCompacting = function(interval, callback, compactionHandler) {
  return callback();
};

ElasticProvider.prototype.stopCompacting = function(callback) {
  return callback();
};

ElasticProvider.prototype.compact = function(callback) {
  return callback();
};


module.exports = ElasticProvider;
