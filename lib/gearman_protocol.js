var net = require('net');
var events = require("events");
var util = require("util");

var defaultServer = {
    address: '127.0.0.1',
    port: 4730
};

exports.GearmanProtocol = GearmanProtocol = function(servers){
    events.EventEmitter.call(this);

    this.servers = [];
    this.socket = null;
    this.currentServer = null;

    this._tempBuf = null;
    this._writeBuf = new Buffer(512); //512 bytes - initial size

    this.commands = {
        CAN_DO : 1,
        CANT_DO : 2,
        RESET_ABILITIES : 3,
        PRE_SLEEP : 4,
        NOOP : 6,
        SUBMIT_JOB : 7,
        JOB_CREATED : 8,
        GRAB_JOB : 9,
        NO_JOB : 10,
        JOB_ASSIGN : 11,
        WORK_STATUS : 12,
        WORK_COMPLETE : 13,
        WORK_FAIL : 14,
        GET_STATUS : 15,
        ECHO_REQ : 16,
        ECHO_RES : 17,
        SUBMIT_JOB_BG : 18,
        ERROR : 19,
        STATUS_RES : 20,
        SUBMIT_JOB_HIGH : 21,
        SET_CLIENT_ID : 22,
        CAN_DO_TIMEOUT : 23,
        ALL_YOURS : 24,
        WORK_EXCEPTION : 25,
        OPTION_REQ : 26,
        OPTION_RES : 27,
        WORK_DATA : 28,
        WORK_WARNING : 29,
        GRAB_JOB_UNIQ : 30,
        JOB_ASSIGN_UNIQ : 31,
        SUBMIT_JOB_HIGH_BG : 32,
        SUBMIT_JOB_LOW : 33,
        SUBMIT_JOB_LOW_BG : 34,
        SUBMIT_JOB_SCHED : 35,
        SUBMIT_JOB_EPOCH : 36
    };

    this.commandsRev = {};

    for(i in this.commands){
        this.commandsRev[this.commands[i]] = i;
    }

    if(servers.forEach === undefined) servers = [servers];

    servers.forEach(function(s){
        this.servers.push({
            address: s.address || defaultServer.address,
            port: s.port || defaultServer.port
        });
    }, this);
};

util.inherits(GearmanProtocol, events.EventEmitter);

GearmanProtocol.prototype.sendCommand = function(cmd, data){

    var dataLen = 0;

    if(data !== undefined){
        if(data.forEach === undefined) data = [data];

        for(i in data){
            if(typeof data[i] !== 'string') data[i] = data[i].toString();
            dataLen += data[i].length;
        }

        dataLen += data.length -1; // for NULLs between data sections
    }

    if(this._writeBuf.length < (dataLen+12)) this._writeBuf = new Buffer(dataLen+12);

    var buf = this._writeBuf.slice(0, dataLen+12);

    buf.write('\0REQ\0\0\0');
    buf[7] = this.commands[cmd];

    buf[8] = (dataLen >> 24) & 0xff;
    buf[9] = (dataLen >> 16) & 0xff;
    buf[10] = (dataLen >> 8) & 0xff;
    buf[11] = dataLen & 0xff;

    if(data !== undefined){
        buf.write(data.join('\0'), 12);
    }

    try {
        this.socket.write(buf);
    } catch (e) {
        console.error('Gearman: socket.write failed: '+e.message);
        this.emit('error', e);
    }
}

GearmanProtocol.prototype.parseCommand = function(buf){

    var _buf = buf;

    if(this._tempBuf){
        _buf = new Buffer(this._tempBuf.length + buf.length);
        this._tempBuf.copy(_buf);
        buf.copy(_buf, this._tempBuf.length);
    }

    if(_buf.length < 12){
        this._tempBuf = _buf;
        return;
    }

    var header = {
        type : _buf.toString('ascii', 1, 4),
        command: this.commandsRev[_buf[7]],
        length: (_buf[8] << 24) + (_buf[9] << 16) + (_buf[10] << 8) + _buf[11]
    };

    if(_buf.length < 12+header.length) {
        this._tempBuf = _buf;
        return;
    } else {
        this._tempBuf = null;
    }

    var data = [];

    start = 12;
    for(i = start; i < 12+header.length; i++){
        if(_buf[i] == 0){
            data.push(_buf.slice(start, i).toString('ascii'));
            start = i+1;
        }
    }
    
    data.push(_buf.slice(start, 12+header.length).toString('ascii'));

    //console.log('header:', header, 'data: ', data);
    
    this.emit(header.command, data);

    if(_buf.length > 12+header.length) this.parseCommand(_buf.slice(12+header.length));
}

GearmanProtocol.prototype.connect = function(){
    this.currentServer = this.servers[Math.round(Math.random()*(this.servers.length-1))];
    
    this.socket = net.createConnection(this.currentServer.port, this.currentServer.address);

    this.socket.on('data', this.parseCommand.bind(this));

    this.socket.on('connect', function(){
        this.socket.setNoDelay(); // increases speed
        this.emit('ready'); // to listen in main app
    }.bind(this));

    this.socket.on('error', function(e){
        this.emit('error', e);
    }.bind(this));
}



