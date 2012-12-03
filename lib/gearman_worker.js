var protocol = require('./gearman_protocol');
var util = require("util");

exports.GearmanWorker = GearmanWorker = function(servers){

    protocol.GearmanProtocol.call(this, servers);

    this.connect();

    this.on('NOOP', function(){
        this.sendCommand('GRAB_JOB');
    });

    this.on('NO_JOB', function(){
        this.sendCommand('PRE_SLEEP');
    });

    this.on('JOB_ASSIGN', function(data){
        this.emit('JOB_ASSIGN_'+data[1], data);
    });
};

util.inherits(GearmanWorker, protocol.GearmanProtocol);

GearmanWorker.prototype.addFunction = function(name, cb, timeout){

    this.on('JOB_ASSIGN_'+name, function(data){
        var handle = data[0];
        data = data.slice(2);

        //process.nextTick(function(){
            var result = cb.call(this, handle, data.length == 1?data[0]:data);
            this.sendCommand('WORK_COMPLETE', [handle].concat(result));
            this.sendCommand('GRAB_JOB');
        //}.bind(this));
        
    });

    if(timeout === undefined || timeout == 0)
        this.sendCommand('CAN_DO', name);
    else
        this.sendCommand('CAN_DO_TIMEOUT', [name].concat(timeout));
    this.sendCommand('GRAB_JOB');
}

GearmanWorker.prototype.removeFunction = function(name){
    this.sendCommand('CANT_DO', name);
    this.removeAllListeners('JOB_ASSIGN_'+name);
}

GearmanWorker.prototype.removeAll = function(){
    this.sendCommand('RESET_ABILITIES');

    // WARNING: this is using private member of EventEmitter so may broke with future versions of node js
    for(e in this._events){
        if(e.match(/^JOB_ASSIGN_/)) this.removeAllListeners(e);
    };
}

GearmanWorker.prototype.sendData = function(handle, data){
    this.sendCommand('WORK_DATA', [handle].concat(data));
}

GearmanWorker.prototype.sendWarning = function(handle, data){
    this.sendCommand('WORK_WARNING', [handle].concat(data));
}

GearmanWorker.prototype.sendFail = function(handle){
    this.sendCommand('WORK_FAIL', handle);
}

GearmanWorker.prototype.sendException = function(handle, data){
    this.sendCommand('WORK_EXCEPTION', [handle].concat(data));
}

GearmanWorker.prototype.sendStatus = function(handle, numerator, denominator){
    this.sendCommand('WORK_STATUS', [handle, numerator, denominator]);
}