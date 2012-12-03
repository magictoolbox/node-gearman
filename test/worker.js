
var gearmanWorker = require('../lib/gearman').createWorker([{
        'address': 'localhost',
        'port': 4730
    }]);

gearmanWorker.once('ready', function(){

    gearmanWorker.addFunction('testwork', function(handle, workload){
        //console.log('handle=', handle, 'workload=', workload);
        return workload;
    });

}).on('error', function(e){
    console.log('Error occured: ', e);
});
