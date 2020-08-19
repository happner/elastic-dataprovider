const mongoComparisonOperators = new Map([
  ['$eq', (field, value) => ProcessValueField(field, value, false)],
  ['$gt', (field, value) => `${field}:>${value}`],
  ['$gte', (field, value) => `${field}:>=${value}`],
  [
    '$in',
    (field, value) => {
      if (!Array.isArray(value))
        throw new Error(`Comparison Query malformed: Expected an Array value for ${field}`);
      if (value.length === 0)
        throw new Error(
          `Comparison Query malformed: Expected a Populated Array value for ${field}`
        );

      let valueString = '(' + value[0];
      for (let val = 1; val < value.length; ++val) {
        valueString += ' OR ' + value[val];
      }
      valueString += ')';
      return `${field}:${valueString}`;
    }
  ],
  ['$lt', (field, value) => `${field}:<${value}`],
  ['$lte', (field, value) => `${field}:<=${value}`],
  ['$ne', (field, value) => `NOT ${field}:${value}`],
  [
    '$nin',
    (field, value) => {
      if (!Array.isArray(value))
        throw new Error(`Comparison Query malformed: Expected an Array value for ${field}`);
      if (!value.length > 0)
        throw new Error(
          `Comparison Query malformed: Expected a Populated Array value for ${field}`
        );
      let valueString = '( NOT ' + value[0];
      for (let val of value) {
        valueString += ' AND NOT' + val;
      }
      valueString += ' OR NOT _exists_)';
      return `${field}:${valueString}`;
    }
  ]
]);

const monogLogicalOperators = new Map([
  [
    '$and',
    (field, value) => {
      let returnStirng = '( ';
      if (!Array.isArray(value))
        throw new Error("Logical Query malfomed: '$and' requires and array");
      for (let val of value) {
        if (returnStirng !== '( ') returnStirng += ' AND ';
        returnStirng += convertCriteria(val, field);
      }

      return returnStirng + ' )';
    }
  ],
  [
    '$not',
    (field, value) => {
      return 'NOT ( ' + ProcessValueField(field, value) + ' )';
    }
  ],
  [
    '$nor',
    (field, value) => {
      let returnStirng = '( NOT';
      if (!Array.isArray(value))
        throw new Error("Logical Query malfomed: '$and' requires and array");
      for (let val of value) {
        if (returnStirng !== '( NOT') returnStirng += ' OR NOT ';
        returnStirng += convertCriteria(val, field);
      }

      return returnStirng + ')';
    }
  ],
  [
    '$or',
    (field, value) => {
      let returnStirng = '( ';
      if (!Array.isArray(value))
        throw new Error("Logical Query malfomed: '$and' requires and array");
      for (let val of value) {
        if (returnStirng !== '( ') returnStirng += ' OR ';
        returnStirng += convertCriteria(val, field);
      }

      return returnStirng + ' )';
    }
  ]
]);

const mongoElementOperators = new Map([
  [
    '$exists',
    (field, value) => {
      if (field === '') throw new Error("Logical Query malfomed: '$and' requires and array");
      if (value) return ` _exists_:${field}`;
      else return ` NOT exists_:${field}`;
    }
  ]
]);

const mongoEvaluationOperators = new Map([
  [
    '$regex',
    (field, value) => {
      if (field === '')
        throw new Error("Evaluation Query malfomed: '$regex' requires a field value");
      if (value.constructor.name === 'RegExp') return `${field}:${value}`;
      else return `${field}:${formatStringCondition(value)}`;
    }
  ]
]);

function isMongoOperator(string) {
  return string.charAt(0) === '$';
}

function formatStringCondition(string) {
  if (string.indexOf('*') > -1) return formatRegex(string);
  else return string;
}

function formatRegex(string) {
  return new RegExp(escapeRegex(string).replace(/\\\*/g, '.*')).toString();
}

function AddCriteriaMongoOperatorAsKey(key, value, keyParrent, field = '') {
  let fieldComplete = '';
  if (keyParrent !== '') fieldComplete = keyParrent + '.';

  fieldComplete += field;
  let comparisonOperation = mongoComparisonOperators.get(key);
  if (comparisonOperation !== undefined) {
    if (fieldComplete === '')
      throw new Error(`Comparison Query malformed: Expected a field  value for ${key}`);
    else return comparisonOperation(fieldComplete, value);
  }
  let logicalOpperation = monogLogicalOperators.get(key);
  if (logicalOpperation !== undefined) {
    return logicalOpperation(fieldComplete, value);
  }
  let elemetnOperators = mongoElementOperators.get(key);
  if (elemetnOperators !== undefined) {
    return elemetnOperators(fieldComplete, value);
  }
  let EvaluationOperators = mongoEvaluationOperators.get(key);
  if (EvaluationOperators !== undefined) {
    return EvaluationOperators(fieldComplete, value);
  }
  throw new Error(`unkown or unsuported MongoOperator '${key}'`);
}

// we are sure key is key
function ProcessValueField(key, value, mongoOperatorAllowed = true) {
  let returnString = '';
  switch (typeof value) {
    case 'string':
      return `${key}:${formatStringCondition(value)}`;

    case 'object':
      if (value.constructor.name === 'RegExp') return `${key}:${value}`;
      for (const [subKey, subValueValue] of Object.entries(value)) {
        if (returnString !== '') returnString += ' AND ';
        if (isMongoOperator(subKey)) {
          if (!mongoOperatorAllowed)
            throw new Error(`Query malformed: mongo operator ${subKey} not allowed in context`);
          returnString += AddCriteria(subKey, subValueValue, '', key);
        } else returnString += AddCriteria(subKey, subValueValue, key, '');
      }
      break;
  }
  return returnString;
}

function AddCriteriaFieldNameAsKey(key, value, keyParrent = '') {
  let field = '';
  if (keyParrent !== '') {
    field = keyParrent + '.';
  }
  field += key;
  return ProcessValueField(field, value);
}

function AddCriteria(criteriaKey, criteriaValue, keyParrent = '', field = '') {
  let queryString = '( ';

  if (!isMongoOperator(criteriaKey)) {
    queryString += AddCriteriaFieldNameAsKey(criteriaKey, criteriaValue, keyParrent);
    // check fieldname as this would be an error
  } else {
    queryString += AddCriteriaMongoOperatorAsKey(criteriaKey, criteriaValue, keyParrent, field);
  }

  queryString += ' ) ';
  return queryString;
}

function escapeRegex(str) {
  if (typeof str !== 'string') throw new TypeError('Expected a string');

  return str.replace(/[|\\{}()[\]^$+*?.]/g, '\\$&');
}

function convertCriteria(options, parrentkey = '') {
  let queryString = '';
  for (const [key, value] of Object.entries(options)) {
    if (queryString !== '') queryString += ' AND ';

    queryString += AddCriteria(key, value, parrentkey);
  }
  return queryString;
}

module.exports = {
  convertOptions: function(options, elasticMessage) {
    if (options.fields) {
      var fieldsClone = JSON.parse(JSON.stringify(options.fields));

      fieldsClone['_id'] = 1;
      fieldsClone['created'] = 1;
      fieldsClone['modified'] = 1;
      fieldsClone['createdBy'] = 1;
      fieldsClone['modifiedBy'] = 1;
      fieldsClone['path'] = 1;
      fieldsClone['data'] = 1;

      elasticMessage._source = Object.keys(options.fields);
    }

    if (options.limit) elasticMessage.body.size = options.limit;

    if (options.offSet) elasticMessage.body.from = options.offSet;

    if (options.sort) {
      elasticMessage.body.sort = [];

      for (var sortFieldName in options.sort) {
        var sortField = {};

        sortField[sortFieldName] = {};

        sortField[sortFieldName]['order'] = options.sort[sortFieldName] === -1 ? 'desc' : 'asc';

        elasticMessage.body.sort.push(sortField);
      }
    }
  },
  // incomplete
  convertCriteria: convertCriteria,
  escapeRegex: escapeRegex
};
