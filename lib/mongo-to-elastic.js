var _ = require('underscore')

module.exports = {

  convertOptions: function (options, elasticMessage) {

    if (options.fields) {

      var fieldsClone = JSON.parse(JSON.stringify(options.fields));

      fieldsClone["_id"] = 1;
      fieldsClone["created"] = 1;
      fieldsClone["modified"] = 1;
      fieldsClone["createdBy"] = 1;
      fieldsClone["modifiedBy"] = 1;
      fieldsClone["path"] = 1;
      fieldsClone["data"] = 1;

      elasticMessage._source = Object.keys(options.fields);
    }

    if (options.limit) elasticMessage.body.size = options.limit;

    if (options.offSet) elasticMessage.body.from = options.offSet;

    if (options.sort) {

      elasticMessage.body.sort = [];

      for (var sortFieldName in options.sort) {

        var sortField = {};

        sortField[sortFieldName] = {};

        sortField[sortFieldName]["order"] = options.sort[sortFieldName] == -1 ? "desc" : "asc";

        elasticMessage.body.sort.push(sortField);
      }
    }
  }
};