
var gearmanClient = require('../lib/gearman').createClient([{
        'address': 'localhost',
        'port': 4730
    }]);

gearmanClient.once('ready', function(){

    /*
    // echo request
    gearmanClient.echo("test message", function(data){
        console.log('echo result: ', data);
    });
     */

    // gearman -w -f testwork -- sed -e 's/[a-z]/a/g'
    gearmanClient.doTask('testwork', ["hello", "from nodejs"], {
        'complete': function(handle, data){console.log(handle + ' complete:', data);},
        'created': function(handle){console.log(handle + ' created');},
        'fail': function(handle, data){console.log(handle + ' fail:', data);},
        'exception': function(handle, data){console.log(handle + ' exception:', data);},
        'data': function(handle, data){console.log(handle + ' data', data);},
        'warning': function(handle, data){console.log(handle + ' warning:', data);},
        'status': function(handle, data){console.log(handle + ' status:', data);}
    });

    /*
    gearmanClient.doTaskBackground('testwork', "hello", {
        'created': function(handle){
            console.log(handle + ' created');
            gearmanClient.getTaskStatus(handle, function(handle, isKnown, isRunning, pNum, pDenom){
                console.log(handle+': isKnown: ', isKnown, ' isRunning: ', isRunning, ' numerator: ', pNum, ' denominator: ', pDenom);
            });
        }
    });
    */

}).on('error', function(e){
    console.log('Error occured: ', e);
});
