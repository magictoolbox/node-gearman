var http = require('http');

var gearmanClient = require('../lib/gearman').createClient([{
    'address': 'localhost',
    'port': 4730
}]);

gearmanClient.once('ready', function(){

    http.createServer(function (req, res) {

        gearmanClient.doTask('testwork', req.url, {
            'complete': function(handle, data){
                res.writeHead(200, {
                    'Content-Type': 'text/plain'
                });
                res.end(data+'\n');
            }
        });

    }).addListener('connection', function(){
        }).listen(8124, "127.0.0.1");

    console.log('Server running at http://127.0.0.1:8124/');

}).on('error', function(e){
    console.log('Error occured: ', e);
});


