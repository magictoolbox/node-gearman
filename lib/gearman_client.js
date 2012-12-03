var protocol = require('./gearman_protocol');
var util = require("util");

exports.GearmanClient = GearmanClient = function(servers){

    protocol.GearmanProtocol.call(this, servers);

    this.jobs = []; //stack of jobs which are not yet confirmed by JOB_CREATED

    this.connect();

    this.on('JOB_CREATED', function(data){
        var cbs = this.jobs.shift();

        if(cbs.created){ // call 'created' callback'
            cbs.created.call(this, data[0]);
        }

        // these will be called only for foreground jobs
        ['complete', 'fail', 'exception', 'data', 'warning', 'status'].forEach(function(i){
            if(cbs[i]){
                this.once('WORK_'+i.toUpperCase()+'_'+data[0], function(data){
                    cbs[i].call(this, data[0], data.length == 2?data[1]:data.slice(2));
                })
            }
        }, this);
    });

    ['COMPLETE', 'FAIL', 'EXCEPTION', 'DATA', 'WARNING', 'STATUS'].forEach(function(i){
        this.on('WORK_'+i, function(data){
            this.emit('WORK_'+i+'_'+data[0], data);
        });
    }, this);

    this.on('STATUS_RES', function(data){
        this.emit('STATUS_RES_'+data[0], data[0], data[1], data[2], data[3], data[4]);
    });

    this.on('error', function(){
        this.jobs = [];
    }.bind(this));
};

util.inherits(GearmanClient, protocol.GearmanProtocol);

GearmanClient.prototype.echo = function(data, cb){
    this.once('ECHO_RES', cb);
    this.sendCommand('ECHO_REQ', data);
}

GearmanClient.prototype.doTask = function(task, _data, cbs, unique, _jobType){

    var data = [task, unique || ''];
    data = data.concat(_data);

    this.jobs.push(cbs);

    var command = 'SUBMIT_JOB';
    if(_jobType !== undefined)
        command += '_'+_jobType;

    this.sendCommand(command, data);
}

GearmanClient.prototype.doTaskLow = function(task, _data, cbs, unique){
    this.doTask(task, _data, cbs, unique, 'LOW');
}

GearmanClient.prototype.doTaskHigh = function(task, _data, cbs, unique){
    this.doTask(task, _data, cbs, unique, 'HIGH');
}

GearmanClient.prototype.doTaskBackground = function(task, _data, cbs, unique){
    this.doTask(task, _data, cbs, unique, 'BG');
}

GearmanClient.prototype.doTaskLowBackground = function(task, _data, cbs, unique){
    this.doTask(task, _data, cbs, unique, 'LOW_BG');
}

GearmanClient.prototype.doTaskHighBackground = function(task, _data, cbs, unique){
    this.doTask(task, _data, cbs, unique, 'HIGH_BG');
}

GearmanClient.prototype.getTaskStatus = function(handle, cb){
    this.once('STATUS_RES_'+handle, cb);
    this.sendCommand('GET_STATUS', handle);
}
