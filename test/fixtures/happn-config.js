
module.exports = {

  get: function(dbPath, testId){

    return {
      services:{
        data:{
          config:{
            datastores:[
              {
                name:'elastic',
                provider:dbPath,
                isDefault:true,
                settings:{
                  host: "http://localhost:9200",
                  indexes: [
                    {
                      index: "happner",
                      body: {
                        "mappings": {}
                      }
                    },
                    {
                      index: "sortedandlimitedindex1",
                      body: {
                        "mappings": {
                          "happner": {
                            "properties": {
                              "data.field1": {"type": "keyword"},
                              "data.item_sort_id": {"type": "integer"}
                            }
                          }
                        }
                      }
                    }],
                  dataroutes: [{
                    pattern: "/1_eventemitter_embedded_sanity/" + testId + "/testsubscribe/data/complex*",
                    index: "sortedandlimitedindex1"
                  },{
                    pattern: "*",
                    index: "happner"
                  }]
                }
              }
            ]
          }
        }
      }
    };
  }
};

