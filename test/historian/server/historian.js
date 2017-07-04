module.exports = {
  pushHistory:function(happnClient, callback){

    var async = require('async');
    var historyJSON = require('../sample_data/historian.json');

    var historyItems = {};

    Object.keys(historyJSON).slice(0, 10).forEach(function(key1){

    //Object.keys(historyJSON).forEach(function(key1){

      var historyItemJSON = historyJSON[key1];

      Object.keys(historyItemJSON).forEach(function(key2) {

        var historyEventsJSON = historyItemJSON[key2];

        historyEventsJSON.forEach(function(datedItem){

          var obj = {value:datedItem.value, __timestamp: datedItem.time * 1000};

          historyItems["/history/" + key1 + "/" + key2 + "/" + datedItem.time] = obj;

          console.log('item:::', obj);
          //historyItems["/history/" + key1 + "/" + key2 + "/" + datedItem.time] = datedItem.value;
          //happnClient.set("/history/" + key1 + "/" + key2 + "/" + datedItem.time, datedItem.value);
        });
      });
    });

    callback();

    async.eachSeries(Object.keys(historyItems), function(itemKey, itemCB){

      console.log('pushing data item:::' + itemKey, new Date(historyItems[itemKey].__timestamp));
      happnClient.set(itemKey, historyItems[itemKey], itemCB)

    }, callback)
  }
};