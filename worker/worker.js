/**
 worker.js

 BrowserSocket MapReduce
 worker node bootstrap and controller.
 Spawns a Web Worker thread which performs the actual work.
 
 Konrad Markus <konker@gmail.com>
 */

var worker = (function() {
    /* the websocket url of the master to which this worker should use */
    var MASTER_WS_URL = 'ws://127.0.0.1:8080/bsmr/';

    /* debug mode on/off */
    var DEBUG = true;

    /* status codes */
    var STATUS_ERROR    = -2;
    var STATUS_STOPPED  = -1;
    var STATUS_INIT     = 1;
    var STATUS_IDLE     = 2;
    var STATUS_MAPPING  = 3;
    var STATUS_REDUCING = 4;

    /* log levels */
    var LOG_INFO  = 1;
    var LOG_ERROR  = 2;
    var LOG_DEBUG = 3;

    /* reduce task split modes */
    var REDUCE_MODE_LOCAL_PRIORITY = 1;
    var REDUCE_MODE_REMOTE_PRIORITY = 2;

    /* modes (FIXME: duplication...) FIXME: remove */
    var MODE_NOR = 1; // normal mode
    var MODE_IVE = 3; // user-interactive mode

    /* Job class */
    function Job(spec) {
        this.spec = spec;
        this.jobId = spec.jobId;
        this.R = spec.R;
        this.M = spec.M;
        this.code = spec.code,
        this.ioCode = {
            input: {
                code: null,
                params: null
            },
            p2p: {
                code: null,
                params: null,
            },
            output: {
                code: null,
                params: null,
            }
        }

        try {
            eval(spec.code);
            this.ioCode.input.params = inputParams;
            this.ioCode.input.code = new inputPlugin(inputParams, worker.map.onData, worker.map.onError);

            this.ioCode.p2p.params = p2pParams;
            this.ioCode.p2p.code = new p2pPlugin(p2pParams, worker.p2p.onData, worker.p2p.onError)

            this.ioCode.output.params = outputParams;
            this.ioCode.output.code = new outputPlugin(outputParams, worker.out.onData, worker.out.onError);
        }
        catch(ex) {
            worker.log(ex, 'log', LOG_ERROR);
        }
    }

    /* MapTask class */
    function MapTask(jobSpec, splitId) {
        this.job = new Job(jobSpec);
        //[FIXME: temp hack]
        //this.job.ioCode.input = new FsInputPlugin({ baseUrl: worker.fs.FS_BASE_URI });
        /*
        this.job.jobCode = "function map(k, data, emit) { var ws = data.split(/\\b/); for (var w in ws) { emit(ws[w], 1); } }; " +
                           "function reduce(k, vs, emit) { var tot = 0; for (var v in vs) { tot += vs[v]; } emit(tot); }";
        */
        //[FIXME: temp hack]

        this.splitId = splitId;
        this.result = null;
    }
    MapTask.prototype.start = function() {
        worker.log('MapTask start() :' + this.splitId, 'log', LOG_INFO);
        //worker.fs.fetchMapData(this.job.jobId, this.splitId);
        this.job.ioCode.input.code.fetchMapData(this.job.jobId, this.splitId);
    }
    MapTask.prototype.onData = function(data) {
        worker.log('MapTask onData() :' + this.splitId, 'log', LOG_DEBUG);
        var m = worker.createMessage(worker.TYPE_DO, {
            action: "map",
            job: this.job.spec,
            splitId: this.splitId,
            data: data
        });
        worker.engine.sendMessage(m);
    }
    MapTask.prototype.done = function(result) {
        worker.log('MapTask done() :' + this.splitId, 'log', LOG_INFO);
        this.result = result;
        worker.map.notifyTaskDone(this)
    }

    MapTaskFactory = function() {}
    /* NOTE: this is a kind of 'static' function */
    MapTaskFactory.createInstance = function(job, splitId) {
        return new worker.MapTask(job, splitId);
    }
    

    /* ReduceTask class */
    function ReduceTask(jobSpec, partitionId) {
        this.splits = [];
        this._mode = worker._reducemode;
        this._looped = false;
        this._ptr = -1;

        this.job = new Job(jobSpec);
        this.partitionId = partitionId;

        if (this.job) {
            for (var i=0; i<this.job.M; i++) {
                this.splits[i] = null;
            }
        }
    }
    ReduceTask.prototype.start = function() {
        worker.log('ReduceTask start() :' + this.partitionId, 'log', LOG_INFO);
        this.doSplit();
    }
    ReduceTask.prototype.getNextSplitId = function() {
        ++this._ptr;
        for (var i=this._ptr; i<this.splits.length; i++) {
            if (this.splits[i]) {
                if (!this.splits[i].isDone()) {
                    if (this._mode == REDUCE_MODE_LOCAL_PRIORITY) {
                        if (worker.reduce.isLocalSplit(this.partitionId, i)) {
                            return i;
                        }
                    }
                    else {
                        if (!worker.reduce.isLocalSplit(this.partitionId, i)) {
                            return i;
                        }
                    }
                }
            }
        }
        if (this._ptr < this.splits.length) {
            return this._ptr;
        }
        else {
            // go around again with in the other mode
            if (!this._looped) {
                this._ptr = -1;
                this._looped = true;

                if (this._mode == REDUCE_MODE_LOCAL_PRIORITY) {
                    this._mode = REDUCE_MODE_REMOTE_PRIORITY;
                }
                else {
                    this._mode = REDUCE_MODE_LOCAL_PRIORITY;
                }
                return this.getNextSplitId();
            }
        }
        return null;
    }
    ReduceTask.prototype.isDone = function() {
        var n = 0; 
        for (var i=0; i<this.splits.length; i++) {
            if (this.splits[i] && this.splits[i].isDone()) {
                ++n;
            }
        }
        return (n == this.job.M);
    },
    ReduceTask.prototype.doSplit = function() {
        if (this.isDone()) {
            this.job.ioCode.output.code.writeOutputFile(this.job.jobId, this.partitionId, worker.reduce.tasks[this.partitionId].getDataJSON());
            return;
        }
        var splitId = this.getNextSplitId();
        worker.log('got next splitId: ' + splitId + '|', 'log', LOG_DEBUG);
        if (splitId !== null) {
            if (worker.reduce.isLocalSplit(this.partitionId, splitId)) {
                worker.reduce.startLocalSplit(this, splitId);
            }
            else {
                worker.reduce.nextSplit(this, splitId);
            }
        }
    }
    ReduceTask.prototype.onSave = function(res) {
        worker.log("reduceTask onSave() :" + res, 'log', LOG_DEBUG);
        this.done();
    }
    ReduceTask.prototype.done = function() {
        worker.log('ReduceTask done() :' + this.partitionId, 'log', LOG_INFO);
        worker.reduce.notifyTaskDone(this)
    }
    ReduceTask.prototype.getDataJSON = function() {
        var outputFile = [];
        for (var i=0; i<this.splits.length; i++) {
            outputFile.push(this.splits[i].result);
        }
        return JSON.stringify(outputFile);
    }

    ReduceTaskFactory = function() {}
    /* NOTE: this is a kind of 'static' function */
    ReduceTaskFactory.createInstance = function(job, partitionId) {
        return new worker.ReduceTask(job, partitionId);
    }

    /* ReduceSplit class */
    function ReduceSplit(job, partitionId, splitId, locations) {
        if (job instanceof Job) {
            this.job = job;
        }
        else {
            this.job = new Job(job);
        }

        this.partitionId = partitionId;
        this.splitId = splitId;
        this.locations = locations;
        this._done = false;
        this.locationPtr = -1;

        this.result = null;
    }
    ReduceSplit.prototype.startLocal = function(data) {
        worker.log('ReduceSplit startLocal() :' + this.partitionId + ',' + this.splitId, 'log', LOG_INFO);
        this.onData(data);
    }
    ReduceSplit.prototype.start = function() {
        worker.log('ReduceSplit start() :' + this.partitionId + ',' + this.splitId, 'log', LOG_INFO);
        this.fetch();
    }
    ReduceSplit.prototype.fetch = function() {
        worker.log('ReduceSplit fetch() :' + this.partitionId + ',' + this.splitId + ',' + this.locationPtr, 'log', LOG_DEBUG);
        this.locationPtr++;
        if (this.locationPtr < this.locations.length) {
            if (worker.server.isLocal(location)) {
                var data = {};
                if (worker.map.tasks[splitId].result[partitionId]) {
                    data = worker.map.tasks[splitId].result[partitionId];
                }
                worker.reduce.tasks[partitionId].splits[splitId].onData(data);
            }
            else {
                this.job.ioCode.p2p.code.fetchIntermediateData(this.job.jobId, this.partitionId, this.splitId, this.locations[this.locationPtr]);
            }
        }
    }
    ReduceSplit.prototype.onData = function(data) {
        worker.log('reduceSplit onData() :' + this.partitionId + ',' + this.splitId, 'log', LOG_DEBUG);
        var m = worker.createMessage(worker.TYPE_DO, {
            action: 'reduce',
            job: this.job.spec,
            partitionId: this.partitionId,
            splitId: this.splitId,
            data: data
        });
        worker.engine.sendMessage(m);
    }
    ReduceSplit.prototype.onError = function(location) {
        worker.log('ReduceSplit onError() :' + this.partitionId + ',' + this.splitId, 'log', LOG_ERROR);
        worker.p2p.unreachable.push(location);
        this.fetch();
    }
    ReduceSplit.prototype.done = function(result) {
        worker.log('ReduceSplit done() :' + this.partitionId + ',' + this.splitId, 'log', LOG_INFO);
        this.result = result;
        this._done = true;
        worker.reduce.notifySplitDone(this);
    }
    ReduceSplit.prototype.isDone = function() {
        return this._done;
    }

    ReduceSplitFactory = function() {}
    /* NOTE: this is a kind of 'static' function */
    ReduceSplitFactory.createInstance = function(job, partitionId, splitId, locations) {
        return new worker.ReduceSplit(job, partitionId, splitId, locations);
    }

    /*
    function FsInputPlugin(spec) {
        this.baseUrl = spec['baseUrl'];

        this.fetchMapData = function(jobId, splitId) {
            var url = this.baseUrl + 'splits/' + jobId + '/' + splitId + '/';

            // connect to fs and retrieve split
            var req = new XMLHttpRequest();  
            req.open('GET', url, true);  
            req.onreadystatechange = function(aEvt) {  
                if (req.readyState == 4) {  
                    if (req.status == 200) {
                        worker.map.onData(jobId, splitId, req.responseText);
                    }
                    else {
                        worker.map.onDataError(req);
                    }
                }  
            };
            req.send(null);  
        }
    }
    */

    return {
        DEBUG: DEBUG,
        MASTER_WS_URL: MASTER_WS_URL,

        /* log levels (FIXME: duplication...) */
        LOG_INFO:  LOG_INFO,
        LOG_ERROR: LOG_ERROR,
        LOG_DEBUG: LOG_DEBUG,

        /* reduce task split modes */
        REDUCE_MODE_LOCAL_PRIORITY: REDUCE_MODE_LOCAL_PRIORITY,
        REDUCE_MODE_REMOTE_PRIORITY: REDUCE_MODE_REMOTE_PRIORITY,

        /* modes (FIXME: duplication...) */
        MODE_NOR: MODE_NOR,
        MODE_IVE: MODE_IVE,

        /* message types (FIXME: duplication...) */
        TYPE_HB: 'HB',
        TYPE_DO: 'DO',
        TYPE_ACK: 'ACK',
        TYPE_FS:  'FS',
        TYPE_P2P: 'P2P',
        TYPE_LOG: 'LOG',
        TYPE_CTL: 'CTL',
        TYPE_ENG: 'ENG',

        /* classes */
        MapTask: MapTask,
        MapTaskFactory: MapTaskFactory,

        ReduceTask: ReduceTask,
        ReduceTaskFactory: ReduceTaskFactory,

        ReduceSplit: ReduceSplit,
        ReduceSplitFactory: ReduceSplitFactory,

        /* reduce task split mode */
        _reducemode: REDUCE_MODE_LOCAL_PRIORITY,

        /* current log level */
        _loglevel: LOG_ERROR,

        /* whether to inmmediately start work */
        _autostart: true,

        /* overall status of the worker node */
        status: null,

        /* track current worker activity */
        active: {
            status: 'idle',
            jobId: null,
            task: null,
            tid: null
        },

        init: function() {
            if (!worker._autostart) {
                worker.log('No autoload. aborting init.', 'log', LOG_INFO);
                return;
            }

            worker.status = STATUS_INIT;

            if (("Worker" in window) == false) {
                worker.setStatus(
                    STATUS_ERROR,
                    'Your browser does not seem to support webworkers. Aborting.'
                );
                return;
            }
            if (("WebSocket" in window) == false) {
                worker.setStatus(
                    STATUS_ERROR,
                    'Your browser does not seem to support websockets. Aborting.'
                );
                return;
            }
            if (("BrowserSocket" in window) == false) {
                worker.setStatus(
                    STATUS_INFO,
                    'Your browser does not seem to support browsersockets. Aborting server.'
                );
            }

            // create a engine thread
            try {
                worker.engine.init();
            }
            catch (ex) {
                worker.setStatus(
                    STATUS_ERROR,
                    'Could not create engine thread: ' + + ex
                );
                return;
            }

            // make a ws connection to the master
            try {
                worker.master.init();
            }
            catch (ex) {
                worker.setStatus(
                    STATUS_ERROR,
                    'Could not connect to master: ' + MASTER_WS_URL + ': ' + ex
                );
                return;
            }

            // open a bs listener
            try {
                worker.server.init();
            }
            catch (ex) {
                worker.setStatus(
                    STATUS_INFO,
                    'Could not open browsersocket: ' + ex
                );
            }

        },
        start: function() {
            // use this if _autostart is false
            worker.heartbeat.start();
            worker.master.greeting();
        },
        step: function() {
            var m = worker.createMessage(worker.TYPE_CTL, {
                action: 'step'
            });
            worker.engine.sendMessage(m);
        },
        idle: function(msg) {
            worker.active.status = 'idle';
            worker.active.jobId = null;
            worker.active.task = null;
            worker.active.tid = null;

            worker.log('In idle state', 'log', LOG_INFO);
        },
        stop: function() {
            worker.master.stop();
            worker.engine.stop();
            worker.server.stop();
            worker.heartbeat.stop();
            worker.setStatus(
                STATUS_STOPPED,
                'stopped'
            );
        },

        /*
            web worker thread that does the actual work */
        engine: {
            thread: null,

            init: function() {
                worker.engine.thread = new Worker('worker-engine.js');
                worker.engine.thread.onmessage = worker.engine.onmessage;
            },
            stop: function() {
                worker.engine.thread.terminate();
            },
            onmessage: function(msg) {
                if (msg.data.type == worker.TYPE_ENG) {
                    switch (msg.data.payload.action) {
                        case 'map':
                            worker.map.tasks[msg.data.payload.splitId].done(msg.data.payload.data); 
                            break;
                        case 'reduce':
                            worker.reduce.tasks[msg.data.payload.partitionId].splits[msg.data.payload.splitId].done(msg.data.payload.data); 
                            break;
                        default:    
                            // swallow for now
                    }
                    worker.log(msg.data, 'e', LOG_DEBUG);
                }
                else if (msg.data.type == worker.TYPE_LOG) {
                    worker.log(msg.data.payload.message, 'log', LOG_DEBUG);
                }
            },
            sendMessage: function(msg) {
                worker.engine.thread.postMessage(msg);
                worker.log(msg, 'we', LOG_DEBUG);
            }
        },

        /*
            websockets connection to the master server */
        master: {
            /* the main ws channel to the master */
            ws: null,

            init: function() {
                /*[TODO: should we have a protocol here?] */
                worker.master.ws = new WebSocket(MASTER_WS_URL, "worker");
                worker.master.ws.onopen = worker.master.onopen;
                worker.master.ws.onclose = worker.master.onclose;
                worker.master.ws.onerror = worker.master.onerror;
            },
            stop: function() {
                if (worker.master.ws) {
                    worker.master.ws.close();
                }
            },
            greeting: function() {
                try {
                    // send the 'socket' message to master
                    var m = worker.createMessage(worker.TYPE_ACK, {
                        action: 'socket',
                        protocol: 'ws',
                        port: worker.server.bs.port,
                        resource: worker.server.bs.resourcePrefix
                    });
                    worker.master.sendMessage(m);
                }
                catch (ex) {
                    worker.setStatus(
                        STATUS_ERROR,
                        'Could not send greeting to master: ' + ex
                    );
                }

                // start accepting server messages from the master
                worker.master.ws.onmessage = worker.master.onmessage;
            },

            /* 
                if we receive a message from the master,
                forward it to the engine thread */
            onmessage: function(e) {
                var msg = worker.readMessage(e.data);
                if (msg.type == worker.TYPE_DO) {
                    switch(msg.payload.action) {
                        case 'mapTask':
                            worker.map.startTask(msg.payload);
                            break;
                        case 'reduceTask':
                            worker.reduce.startTask(msg.payload);
                            break;
                        case 'reduceSplit':
                            worker.reduce.startSplit(msg.payload);
                            break;
                        case 'idle':
                            worker.idle(msg.payload);
                            break;
                        default:    
                            // swallow for now
                    }
                }
                else if (msg.type == worker.TYPE_P2P) {
                    //worker.reduce.startUploaded(msg.payload);
                    worker.p2p.exec(msg.payload);
                }
                else if (msg.type == worker.TYPE_CTL) {
                    worker.control.exec(msg.payload);
                }
                /*
                else if (msg.type == worker.TYPE_FS) {
                    worker.fs.exec(msg.payload);
                }
                */
                worker.log(msg, 'm', LOG_DEBUG);
            },
            onopen: function(e) { 
                if (worker._autostart) {
                    worker.master.greeting();
                }

                // successful init
                worker.setStatus(STATUS_IDLE, 'init complete');
            },
            onclose: function(e) {
                /* FIXME: why does this cause an error? */
                worker.log('ws:close', 'log', LOG_DEBUG);
                worker.stop();   
            },
            onerror: function(e) {
                /*[TODO]*/
                worker.log('ws:error', 'log', LOG_ERROR);
            },
            sendMessage: function(msg) {
                worker.master.sendMessageExec(msg, false);
            },
            sendMessageExec: function(msg, _no_reset_hb) {
                worker.master.ws.send(JSON.stringify(msg));
                worker.log(msg, 'w', 'log', LOG_DEBUG);
                if (!_no_reset_hb) {
                    worker.heartbeat.reset();
                }
            }
        },

        heartbeat: {
            /*TODO:
                - restart hb timeout with every ack?
                - delay the hb
            */
            HB_INTERVAL_MS: 10000,
            tid: null,

            start: function() {
                clearInterval(worker.heartbeat.tid);
                worker.heartbeat.tid = setInterval(worker.heartbeat.heartbeat, worker.heartbeat.HB_INTERVAL_MS);
                worker.log('Heartbeat started', 'log', LOG_INFO);
            },
            heartbeat: function() {
                var m = worker.createMessage(worker.TYPE_HB, {
                    action: worker.active.status,
                    jobId: worker.active.jobId
                });
                worker.master.sendMessageExec(m, true);
            },
            reset: function() {
                worker.heartbeat.stop();
                worker.heartbeat.start();
            },
            stop: function() {
                clearInterval(worker.heartbeat.tid);
                worker.log('Heartbeat stopped', 'log', LOG_INFO);
            }
        },

        /*
            map tasks */
        map: {
            tasks: [],

            onData: function(jobId, splitId, data) {
                worker.log('map.onData: ' + jobId + ', ' + splitId, 'log', LOG_DEBUG);
                worker.log('map.onData: ' + data, 'log', LOG_DEBUG);
                if (typeof(worker.map.tasks[splitId].onData) == 'function') {
                    worker.map.tasks[splitId].onData(data);
                }
            },
            onError: function(req) {
                /*[FIXME: beter error handling?]*/
                worker.log('fs fetchMapData() failed: ' + req.status, 'log', LOG_ERROR);
            },
            startTask: function(spec) {
                /*[FIXME: check for job/split safety]*/
                var t = worker.MapTaskFactory.createInstance(spec.job, spec.mapStatus.splitId, 'FIXME_DATASRC');
                worker.map.tasks[t.splitId] = t;
                worker.map.tasks[t.splitId].start();
                worker.active.jobId = t.job.jobId;
                worker.active.task = t;
                worker.active.status = 'mapTask';
            },
            notifyTaskDone: function(t) {
                var m = worker.createMessage(worker.TYPE_ACK, {
                    action: 'mapTask',
                    mapStatus: {
                        splitId: t.splitId
                    },
                    reduceStatus: null, //TODO
                    jobId: t.job.jobId
                });
                worker.master.sendMessage(m);
            }
        },

        reduce: {
            //FIXME: there should only be one reduce task at any one time?
            tasks: [],

            startTask: function(spec) {
                var t = worker.ReduceTaskFactory.createInstance(spec.job, spec.reduceStatus.partitionId);
                worker.reduce.tasks[t.partitionId] = t;
                worker.reduce.tasks[t.partitionId].start();
                worker.active.jobId = t.job.jobId;
                worker.active.task = t;
                worker.active.status = 'reduceTask';
            },
            startLocalSplit: function(reduceTask, splitId) {
                var t = worker.ReduceSplitFactory.createInstance(reduceTask.job, reduceTask.partitionId, splitId, null);
                worker.reduce.tasks[t.partitionId].splits[t.splitId] = t;
                worker.reduce.tasks[t.partitionId].splits[t.splitId].startLocal(worker.reduce.getLocalSplit(t.partitionId, t.splitId));
                worker.active.jobId = t.job.jobId;
                worker.active.task = t;
                worker.active.status = 'reduceTask';
            },
            startSplit: function(spec) {
                var t = worker.ReduceSplitFactory.createInstance(spec.job, spec.reduceStatus.partitionId, spec.reduceStatus.splitId, spec.reduceStatus.locations);
                worker.reduce.tasks[t.partitionId].splits[t.splitId] = t;
                worker.reduce.tasks[t.partitionId].splits[t.splitId].start();
                worker.active.jobId = t.job.jobId;
                worker.active.task = t;
                worker.active.status = 'reduceTask';
            },
            nextSplit: function(t, splitId) {
                var m = worker.createMessage(worker.TYPE_ACK, {
                    action: 'reduceSplit',
                    reduceStatus: {
                        partitionId: t.partitionId,
                        splitId: splitId
                    },
                    unreachable: worker.p2p.unreachable,
                    jobId:t.job.jobId});
                worker.master.sendMessage(m);
            },
            notifySplitDone: function(t) {
                worker.reduce.tasks[t.partitionId].doSplit();
            },
            notifyTaskDone: function(t) {
                var m = worker.createMessage(worker.TYPE_ACK, {
                    action: 'reduceTask',
                    reduceStatus: {
                        partitionId: t.partitionId
                    },
                    unreachable: worker.p2p.unreachable,
                    jobId: t.job.jobId
                });
                worker.master.sendMessage(m);
            },
            isLocalSplit: function(partitionId, splitId) {
                if (worker.map.tasks[splitId] &&
                    worker.map.tasks[splitId].result &&
                    worker.map.tasks[splitId].result[partitionId]) {

                    // if local map task results for this split exists return true
                    return true;
                }
                return false;
            },
            getLocalSplit: function(partitionId, splitId) {
                if (worker.reduce.isLocalSplit(partitionId, splitId)) {
                    // if local map task results for this split exists return it
                    return worker.map.tasks[splitId].result[partitionId];
                }
                return null;
            }
        },

        /*
            interaction with the output file system */
        out: {
            //FS_BASE_URI: 'http://localhost:8080/fs/',

            onData: function(partitionId, response) {
                if (typeof(worker.reduce.tasks[partitionId].onSave) == 'function') {
                    worker.log('fs writeOutputFile(): ' + partitionId + ' OK: ' + response, 'log', LOG_INFO);
                    worker.reduce.tasks[partitionId].onSave(response);
                }
            },
            onError: function(req) {
                worker.log('fs writeOutputFile() failed: ' + req.status, 'log', LOG_ERROR);
            },

            /*
            writeOutputFile: function(jobId, partitionId) {
                // connect to fs and upload partition
                var req = new XMLHttpRequest();  
                req.open('POST', worker.fs.getPartitionUrl(jobId, partitionId), true);  
                req.onreadystatechange = function(aEvt) {  
                    if (req.readyState == 4) {  
                        if (req.status == 200) {
                            if (typeof(worker.reduce.tasks[partitionId].onSave) == 'function') {
                                worker.reduce.tasks[partitionId].onSave(req.responseText);
                            }
                        }
                        else {
                            worker.log('fs writeOutputFile() failed: ' + req.status, 'log', LOG_ERROR);
                        }
                    }  
                };
                req.overrideMimeType('text/plain');
                req.send(worker.reduce.tasks[partitionId].getDataJSON());
            },
            */

            /*
            fetchMapData: function(jobId, splitId) {
                // connect to fs and retrieve split
                var req = new XMLHttpRequest();  
                req.open('GET', worker.fs.getSplitUrl(jobId, splitId), true);  
                req.onreadystatechange = function(aEvt) {  
                    if (req.readyState == 4) {  
                        if (req.status == 200) {
                            if (typeof(worker.map.tasks[splitId].onData) == 'function') {
                                worker.map.tasks[splitId].onData(req.responseText);
                            }
                        }
                        else {
                            worker.log('fs fetchMapData() failed: ' + req.status, 'log', LOG_ERROR);
                        }
                    }  
                };
                req.send(null);  
            },
            */

            /*
            getPartitionUrl: function(jobId, partitionId) {
                worker.log(worker.fs.FS_BASE_URI + 'partitions/' + jobId + '/' + partitionId + '/', 'log', LOG_DEBUG);
                return worker.fs.FS_BASE_URI + 'partitions/' + jobId + '/' + partitionId + '/';
            },
            getSplitUrl: function(jobId, splitId) {
                worker.log(worker.fs.FS_BASE_URI + 'splits/' + jobId + '/' + splitId + '/', 'log', LOG_DEBUG);
                return worker.fs.FS_BASE_URI + 'splits/' + jobId + '/' + splitId + '/';
            }
            */
        },

        /*
            interaction with other worker nodes via browsersockets */
        p2p: {
            unreachable: [],

            onData: function(jobId, partitionId, splitId, data) {
                worker.log('p2p.onData: ' + jobId + ', ' + partitionId + ', ' + splitId, 'log', LOG_DEBUG);
                worker.log('p2p.onData: ' + data, 'log', LOG_DEBUG);
                if (typeof(worker.reduce.tasks[partitionId].splits[splitId].onData) == 'function') {
                    worker.reduce.tasks[partitionId].splits[splitId].onData(data);
                }
            },
            onError: function(jobId, partitionId, splitId, location) {
                /*[FIXME: beter error handling?]*/
                worker.log('p2p fetchIntermediateData() failed: ' + location, 'log', LOG_ERROR);
                worker.reduce.tasks[msg.payload.partitionId].splits[msg.payload.splitId].onError(location);
            },

            /*
            fetchIntermediateData: function(jobId, partitionId, splitId, location) {
                worker.log('p2p.fetchIntermediateData() :' + partitionId + ',' + splitId + ',' + location, 'log', LOG_DEBUG);

                var msg = null;
                if (worker.server.isLocal(location)) {
                    var data = worker.map.tasks[splitId].result[partitionId];
                    worker.reduce.tasks[partitionId].splits[splitId].onData(data);
                }
                else {
                    // go over network to get the data
                    var ws = new WebSocket(location);
                    ws.onopen = function() {
                        var m = worker.createMessage(worker.TYPE_P2P, {
                            action: 'download',
                            jobId: jobId,
                            partitionId: partitionId,
                            splitId: splitId,
                            location: location
                        });
                        this.send(JSON.stringify(m));
                    }
                    ws.onmessage = function(e) {
                        msg = worker.readMessage(e.data);
                        worker.log(e.data, 'log', LOG_DEBUG);
                    }
                    ws.onclose = function() {
                        if (msg) {
                            if (msg.payload.action == 'upload') {
                                if (typeof(worker.reduce.tasks[msg.payload.partitionId].splits[msg.payload.splitId].onData) == 'function') {
                                    worker.reduce.tasks[msg.payload.partitionId].splits[msg.payload.splitId].onData(msg.payload.data);
                                }
                            }
                        }
                    }
                    ws.onerror = function() {
                        worker.reduce.tasks[msg.payload.partitionId].splits[msg.payload.splitId].onDataError(location);
                    }
                }
            }
            */
        },

        /*
            browsersocket connection to other worker nodes */
        server: {
            /* the browsersocket for communicating with other worker nodes */
            bs: null,

            init: function() {
                worker.server.bs = new BrowserSocket(worker.server.handlerFactory);
                worker.server.bs.onerror = worker.server.onerror;
            },
            stop: function() {
                worker.server.bs.stop();
            },

            handlerFactory: function(req) {
                return new worker.server.handler(req);
            },
            handler: function(req) {
                this.onmessage = function(e) {
                    /*[TODO]*/
                    var msg = worker.readMessage(e.data);
                    if (msg.type == worker.TYPE_P2P) {
                        switch (msg.payload.action) {
                            case 'download':
                                var data = {};
                                if (worker.map.tasks[msg.payload.splitId].result[msg.payload.partitionId]) {
                                    data = worker.map.tasks[msg.payload.splitId].result[msg.payload.partitionId];
                                }
                                // send back the partition/split result
                                var m = worker.createMessage(worker.TYPE_P2P, {
                                    action: 'upload',
                                    jobId: msg.payload.jobId,
                                    partitionId: msg.payload.partitionId,
                                    splitId: msg.payload.splitId,
                                    data: data
                                });
                                this.send(JSON.stringify(m));
                                break;
                            default:
                                // swallow for now
                        }
                    }
                    worker.log('bs:handler:onmessage: ' + e.data, 'log', LOG_DEBUG);
                    this.close();
                }
                this.onopen = function(e) { 
                    /*[TODO]*/
                    worker.log('bs:handler:open', 'log', LOG_DEBUG);
                }
                this.onclose = function(e) {
                    /*[TODO]*/
                    worker.log('bs:handler:close', 'log', LOG_DEBUG);
                }
                this.onerror = function(e) {
                    /*[TODO]*/
                    worker.log('bs:handler:error', 'log', LOG_ERROR);
                }
            },
            onerror: function(e) {
                /*[TODO]*/
                worker.log('bs:error', 'log', LOG_ERROR);
            },
            isLocal: function(addr) {
                if (addr) {
                    return (addr.toString().search(worker.server.bs.resourcePrefix) != -1);
                }
                return false;
            }
        },

        /* utils and helpers */
        readMessage: function(s) {
            return JSON.parse(s);
        },
        createMessage: function(type, spec) {
            var ret = {
                payload: {}
            };
            ret.type = type;
            for (var p in spec) {
                if (spec.hasOwnProperty(p)) {
                    ret.payload[p] = spec[p];
                }
            }
            return ret;
        },
        setStatus: function(status, msg) {
            if (msg) {
                worker.log(msg, 'log', LOG_INFO);
            }
            worker.status = status;
        },
        util: {
            esc: function(s) {
                return s.replace(/&/g, '&amp;')
                        .replace(/</g, '&lt;')
                        .replace(/>/g, '&gt;');
            }
        },
        log: function(s, type, level) {
            if (worker.DEBUG) {
                if (level <= worker._loglevel) {
                    if (typeof(console) != 'undefined' && typeof(console.log) == 'function') {
                        console.log(s);
                    }
                }
            }
        }
    }
})();
window.addEventListener('load', worker.init, false);

/*[TODO: scrap] */
/*
function _gP(o) {
    var s = '[';
    for (var p in o) {
        s += p + '->' + o[p] + ', ';
    }
    return (s + ']');
}
*/
