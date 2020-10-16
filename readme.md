happner elastic dataprovider
----------------------------
*This dataprovider provides the ability to run happner/happn instances off of elasticsearch instead of mongo or nedb*

### NB: still in active dev, so don't use in production yet.

### installation instructions:

```bash
#install deps
npm install happner-elastic-dataprovider
#test run - most should pass
mocha test/func

#now run the historian data upload - this is demo code
node test/historian/server/start.js
```

#### Installing ElasticSearch and running ElasticSearch

```bash
curl -L -O https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.5.1-linux-x86_64.tar.gz
tar -xvf elasticsearch-5.5.1-linux-x86_64.tar.gz 
./elasticsearch-5.5.1/bin/elasticsearch


```
### configuration:

```javascript

//single use config - everything goes into elasticsearch
var config = {
  happn:{
    services:{
      data:{
        config:{
          datastores:[
            {
              name:'elastic',
              provider:'happner-elastic-dataprovider',
              settings:{"host":"localhost:9200"},
              isDefault:true
            }
          ]
        }
      }
    }
  }
};

//dual config, send all items starting with the path /history/ to elastic search, all others go to the default nedb instance
var config = {
  happn:{
    services:{
      data:{
        config:{
          datastores:[
            {
              name:'elastic',
              provider:'happner-elastic-dataprovider',
              settings:{"host":"localhost:9200"},
              patterns:["/history/*"]
            },
            {
              name:'happn',
              isDefault:true
            }
          ]
        }
      }
    }
  }
};

//then create happner instance as usual:

var Happner = require('happner');

Happner.create(config)

.then(function(mesh) {
// got running mesh
})

.catch(function(error) {
console.error(error.stack || error.toString())
process.exit(1);
});

```

Happner setup instructions in more detail [here](https://github.com/happner/happner/blob/master/docs/walkthrough/the-basics.md).

## Supported Mongo Style Search Parameters 

* $eq
* $gt
* $gte
* $in
* $lt
* $lte
* $ne
* $nin
* $and
* $not
* $nor
* $or
* $exists
* $regex

### Limitations

#### Embedded documents 
Embedded documents works slightly different than traditional mongo queries
Consider the following document:


`
{
 size:{
        h: 14, 
        w: 21, 
        uom: "cm" 
    }
}
`

Field order does not matter for this provider while they matter for regular mongo. Given the following query, mongo would not match while this provider will. 

`
{
 size:{
        w: 21, 
        h: 14, 
        uom: "cm" 
    }
}
`

When a document is specified as an query mongo requires the document to be a precise match. This provider only requires the specified fields to match. The following query will match using this provider but won't match using mongo

` {
  size:{
         w: 21, 
         h: 14 
              }
 }`