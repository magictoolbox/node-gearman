client = require('./gearman_client.js');
worker = require('./gearman_worker.js');

exports.createClient = function(servers){
    return new client.GearmanClient(servers);
};

exports.createWorker = function(servers){
    return new worker.GearmanWorker(servers);
};
