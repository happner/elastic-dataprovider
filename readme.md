happner elastic dataprovider
----------------------------
*This dataprovider provides the ability to run happner/happn instances off of elasticsearch instead of mongo or nedb*


### installation instructions:

```bash
#install deps
npm install happner-elastic-dataprovider
#test run - most should pass
mocha test/func

#now run the historian data upload - this is demo code
node test/historian/server/start.js
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