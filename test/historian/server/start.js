var config = require('../../private/config');

var Happner = require('happner-2')
  , path = require('path')
  , service
  ;

var __started = false;

var terminate = function (code) {

  console.log('::: Terminating service...');
  console.log('::: Running termination tasks, 1 minute deadline...');

  if (__started) {

    service.stop(function(e){

      if (e) {
        console.log('::: Error stopping services: ' + e);
        process.exit(1);
      }
      process.exit(code);
    });

  } else process.exit(code);
};

process.on('SIGTERM', function (code) {
  return terminate(code);
});

process.on('SIGINT', function (code) {
  return terminate(code);
});

Happner.create(config)

  .then(function(serviceInstance){

    service = serviceInstance;
    __started = true;

    var historian = require('./historian');

    console.log('service started...');

    historian.pushHistory(service._mesh.data, function(e){

      if (e) return console.log('historian pushed error...', e.toString());

      console.log('historian pushed...');
    });
  })
  .catch(function(e){
    console.log('failed to start service: ', e.toString());
    terminate(1);
  });
