var mongoToElastic = require('./lib/mongo-to-elastic')
  , sift = require('sift')
  , async = require('async')
  , traverse = require('traverse')
  ;

function ElasticProvider(config) {

  if (config.cache) {

    if (config.cache === true) config.cache = {};

    if (!config.cache.cacheId) config.cache.cacheId = config.name;
  }

  if (!config.defaultIndex) config.defaultIndex = 'happner';

  if (!config.defaultType) config.defaultType = 'happner';

  Object.defineProperty(this, 'config', {value: config});

  Object.defineProperty(this, '__dynamicRoutes', {value: {}});
}

ElasticProvider.prototype.__wildcardMatch = function (pattern, matchTo) {

  var regex = new RegExp(pattern.replace(/[*]/g, '.*'));
  var matchResult = matchTo.match(regex);

  if (matchResult) return true;

  return false;
}

ElasticProvider.prototype.__createDynamicObject = function (dynamicParts, data) {

  try{

    Object.keys(dynamicParts.values).forEach(function(dynamicValueKey){

      if (dynamicParts.values[dynamicValueKey] == null) return;

      var value = dynamicParts.values[dynamicValueKey];

      dynamicParts.fields.forEach(function(dynamicField){

        if (dynamicField.name == dynamicValueKey){

          if (['integer', 'date'].indexOf(dynamicField.type) > -1)
            value = parseInt(value);

          else if (['long', 'double', 'float', 'half_float', 'scaled_float'].indexOf(dynamicField.type) > -1)
            value = parseFloat(value);

          else if (dynamicField.type == 'boolean') value = !!(value==true||value==1);

          else if (dynamicField.type != 'string') throw new Error('unable to create dynamic value for type: ' + dynamicField.type + ', can only do basic types: string, integer, date (utc only), long, double, float, half_float, scaled_float');
        }
      });

      data[dynamicValueKey] = value;
    });

    return data;

  }catch(e){

    throw new Error('failed to generate dynamic object', e);
  }
};

ElasticProvider.prototype.__createDynamicIndex = function (dynamicParts, callback) {

  var _this = this;

  if (/[A-Z-+_*$@#$%&!]/.test(dynamicParts.index)) return callback(new Error('bad dynamic index name: ' + dynamicParts.index + ' must be lowercase with no special characterssuch as A-Z-+_*$@#$%&!'));

  var indexJSON = _this.__buildIndexObj({
      index: dynamicParts.index,
      type: dynamicParts.type
    }
  );

  dynamicParts.fields.forEach(function(dynamicField){

    indexJSON.body.mappings[dynamicParts.type].properties[dynamicField.name] = {type:dynamicField.type};
  });

  _this.__createIndex(dynamicParts.index, indexJSON, callback);
};

ElasticProvider.prototype.__prepareDynamicIndex = function (dataStoreRoute, path, data, callback) {

  var _this = this;

  var indexSegments = dataStoreRoute.pattern.split('/');

  var pathSegments = path.split('/');

  var dynamicParts = {fields: [], values:{}};

  var indexCreated = false;

  var createdAlready = function(){

    if (dynamicParts.index != null && dynamicParts.type != null && _this.__dynamicRoutes[dynamicParts.index + '_' + dynamicParts.type] != null) {
      indexCreated = true;
      return true;
    }
    return false;
  };

  indexSegments.every(function (segment, segmentIndex) {

    if (segment == "{{index}}") {

      dynamicParts.index = pathSegments[segmentIndex];

      return !createdAlready();
    }

    else if (segment == "{{type}}") {

      dynamicParts.type = pathSegments[segmentIndex];

      return !createdAlready();
    }

    else if (segment.indexOf("{{") == 0) {

      var fieldSegment = segment.replace("{{", "").replace("}}", "");

      var dynamicField = {type: "string", name: fieldSegment};

      if (segment.indexOf(":") > 0) {

        dynamicField.type = fieldSegment.split(":")[1];
        dynamicField.name = fieldSegment.split(":")[0];
      }

      dynamicParts.fields.push(dynamicField);
      dynamicParts.values[dynamicField.name] = pathSegments[segmentIndex];
    }

    return true;
  });

  if (data) data = _this.__createDynamicObject(dynamicParts, data);

  if (!indexCreated) {

    _this.__createDynamicIndex(dynamicParts, function (e) {

      if (e) return callback(e);

      _this.__dynamicRoutes[dynamicParts.index + '_' + dynamicParts.type] = dynamicParts;

      callback(null, dynamicParts, data);
    });

  } else callback(null, dynamicParts, data);
};

ElasticProvider.prototype.__getRoute = function (path, data, callback) {

  var _this = this;

  var route = null;

  if (typeof data == 'function') {
    callback = data;
    data = null;
  }

  _this.config.dataroutes.every(function (dataStoreRoute) {

    var pattern = dataStoreRoute.pattern;

    if (dataStoreRoute.dynamic){
      pattern = dataStoreRoute.pattern.split('{{')[0] + '*';
    }

    if (_this.__wildcardMatch(pattern, path)) {

      route = dataStoreRoute;

      return false;

    } else return true;
  });

  if (!route) return callback(new Error('route for path ' + path + ' does not exist'));

  if (route.dynamic) {

    _this.__prepareDynamicIndex(route, path, data, function (e, dynamicRoute, data) {

      if (e) return callback(e);

      callback(null, dynamicRoute, data);
    });

  } else {

    return callback(null, {index: route.index, type: _this.config.defaultType}, data);
  }
};


ElasticProvider.prototype.__createIndex = function (index, indexConfig, callback) {

  var _this = this;

  var doCallback = function (e, response) {
    if (e) return callback(new Error('failed creating index' + index + ':' + e.toString(), e));
    callback(null, response);
  };

  _this.db.indices.exists({
    'index': index
  }, function (e, res) {

    if (e) return doCallback(e);

    if (res === false) {

      _this.db.indices.create(indexConfig, doCallback);

    } else return doCallback(null, res);
  });
};

ElasticProvider.prototype.__buildIndexObj = function (indexConfig) {

  var _this = this;

  if (indexConfig.index == null) indexConfig.index = _this.config.defaultIndex;

  if (indexConfig.type == null) indexConfig.type = _this.config.defaultType;

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

      if (indexJSON.body.mappings[indexConfig.type].properties[fieldName] == null)
        indexJSON.body.mappings[indexConfig.type].properties[fieldName] = indexConfig.body.mappings[indexConfig.type].properties[fieldName];
    });

  return indexJSON;
};

ElasticProvider.prototype.__createIndexes = function (callback) {

  var _this = this;

  if (!_this.config.indexes) _this.config.indexes = [];

  var defaultIndexFound = false;

  _this.config.indexes.forEach(function (indexConfig) {

    if (indexConfig.index === _this.config.defaultIndex) defaultIndexFound = true;
  });

  var indexJSON = _this.__buildIndexObj({
      index: _this.config.defaultIndex,
      type: _this.config.defaultType
    }
  );

  if (!defaultIndexFound) {
    _this.config.indexes.push(indexJSON);
  }

  async.eachSeries(_this.config.indexes, function (index, indexCB) {

    if (index.index != _this.defaultIndex) indexJSON = _this.__buildIndexObj(index);

    _this.__createIndex(index.index, indexJSON, indexCB);

  }, function (e) {

    if (e) return callback(e);

    if (!_this.config.dataroutes) _this.config.dataroutes = [];

    //last route goes to default index

    var defaultRouteFound = false;

    _this.config.dataroutes.forEach(function (route) {
      if (route.pattern == '*') defaultRouteFound = true;
    });

    if (!defaultRouteFound) _this.config.dataroutes.push({pattern: '*', index: _this.config.defaultIndex});

    callback();
  });
};

ElasticProvider.prototype.initialize = function (callback) {

  var _this = this;

  var elasticsearch = require('elasticsearch');

  try {

    var client = new elasticsearch.Client(_this.config);

    client.ping({
      requestTimeout: 30000
    }, function (e) {

      if (e) return callback(e);

      Object.defineProperty(_this, 'db', {value: client});

      _this.__createIndexes(callback);
    });
  } catch (e) {
    callback(e);
  }
};

ElasticProvider.prototype.findOne = function (criteria, fields, callback) {

  var path = criteria.path;

  delete criteria.path;

  this.find(path, {options:fields, criteria:criteria}, function (e, results) {

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

ElasticProvider.prototype.__filter = function (criteria, items) {
  try {
    return sift(criteria, items);
  } catch (e) {
    throw new Error('filter failed: ' + e.toString(), e);
  }
};

ElasticProvider.prototype.find = function (path, parameters, callback) {

  var _this = this;

  _this.__getRoute(path, function (e, route) {

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

    var returnType = path.indexOf('*'); //0,1 == array -1 == single

    if (returnType == 0) {

      elasticMessage.body["query"]["bool"]["must"].push({
        "regexp": {
          "path": '^' + path.replace(/[*]/g, '.*')
        }
      });
    } else if (returnType > 0) {

      elasticMessage.body["query"]["bool"]["must"].push({
        "regexp": {
          "path": path.replace(/[*]/g, '.*')
        }
      });
    } else {
      elasticMessage.body["query"]["bool"]["must"].push({
        "terms": {
          "_id": [path]
        }
      });
    }

    _this.db.search(elasticMessage)

      .then(function (resp) {

        if (resp.hits && resp.hits.hits && resp.hits.hits.length > 0) {

          var found = resp.hits.hits;

          if (parameters.criteria)  found = _this.__filter(_this.__parseFields(parameters.criteria), found);


          callback(null, found);

        } else callback(null, []);

      })
      .catch(function (e) {
        callback(e);
      });
  });

};

ElasticProvider.prototype.update = function (criteria, data, options, callback) {

  return this.db.update(criteria, data, options, callback);
};

ElasticProvider.prototype.transform = function (dataObj, meta) {

  var elasticMeta = {
    _type:dataObj._type,
    _id:dataObj._id,
    _score:dataObj._score
  };

  if (dataObj._source) dataObj = dataObj._source;

  dataObj._id = elasticMeta._id;
  dataObj._type = elasticMeta._type;
  dataObj._score = elasticMeta._score;

  var transformed = {data:dataObj.data};

  if (!meta) {

    meta = {};

    if (dataObj.created) meta.created = dataObj.created;

    if (dataObj.modified) meta.modified = dataObj.modified;

    if (dataObj.modifiedBy) meta.modifiedBy = dataObj.modifiedBy;

    if (dataObj.createdBy) meta.createdBy = dataObj.createdBy;
  }

  transformed._meta = meta;

  if (!dataObj._id){

    transformed._meta._id = transformed.path;
  } else {

    transformed._meta.path = dataObj._id;
    transformed._meta._id = dataObj._id;
  }

  if (dataObj._tag) transformed._meta.tag = dataObj._tag;

  return transformed;
};

ElasticProvider.prototype.transformAll = function (items) {

  var _this = this;

  return items.map(function (item) {

    return _this.transform(item, null);
  })
};

// ElasticProvider.prototype.transform = function (result, meta) {
//
//   var transformed = {};
//
//   transformed._meta = meta;
//   transformed.data = result._source.data;
//
//   return transformed;
// };
//
// ElasticProvider.prototype.transformAll = function (results) {
//
//   var _this = this;
//   var transformed = [];
//
//   results.forEach(function (result) {
//     transformed.push(_this.transform(result));
//   });
//
//   return transformed;
// };

ElasticProvider.prototype.__parseFields = function (fields) {

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

        if (_thisNode.key.indexOf('data.') == 0) _thisNode.parent.node['_source.' + _thisNode.key] = value;
        //prepend with data.
        else _thisNode.parent.node['_source.data.' + _thisNode.key] = value;

        return _thisNode.remove();
      }
    }
  });

  return fields;
};

ElasticProvider.prototype.__getMeta = function (response) {

  var meta = {
    created: response.created,
    modified: response.modified,
    modifiedBy: response.modifiedBy,
    timestamp: response.timestamp,
    path: response.path,
    _id: response.path
  };

  return meta;
};

ElasticProvider.prototype.upsert = function (path, setData, options, dataWasMerged, callback) {

  var _this = this;

  var modifiedOn = Date.now();

  var timestamp = setData.data.timestamp ? setData.data.timestamp : modifiedOn;

  this.__getRoute(path, setData.data, function (e, route, routeData) {

    if (e) return callback(e);

    setData.data = routeData;

    var index = route.index;

    var elasticMessage = {
      "index": index,
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
      refresh: true
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
  });
};

ElasticProvider.prototype.count = function (message, callback) {

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

  this.db.count(countMessage, function (e, response) {

    if (e) return callback(e);

    callback(null, response.count);
  });
};

ElasticProvider.prototype.remove = function (path, callback) {

  var _this = this;

  var multiple = path.indexOf('*') > -1;

  var deletedCount = 0;

  this.__getRoute(path, function (e, route) {

    var elasticMessage = {
      index: route.index,
      type: route.type,
      refresh: true
    };

    var handleResponse = function (e, response) {

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

      if (multiple) _this.db.deleteByQuery(elasticMessage, handleResponse);

      else _this.db.delete(elasticMessage, handleResponse);
    });
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

ElasticProvider.prototype.stop = function (callback) {
  this.db.close();
  callback();
};

module.exports = ElasticProvider;
