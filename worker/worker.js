/**
 worker.js

 BrowserSocket MapReduce
 worker node bootstrap and controller.
 Spawns a Web Worker thread which performs the actual work.
 
 Konrad Markus <konker@gmail.com>
 */

var konk = (function() {
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

    /* modes (FIXME: duplication...) */
    var MODE_NOR = 1; // normal mode
    var MODE_IVE = 3; // user-interactive mode

    /* MapTask class */
    function MapTask(job, splitId) {
        this.job = job;
        this.splitId = splitId;
        this.result = null;
    }
    MapTask.prototype.start = function() {
        konk.log('MapTask start() :' + this.splitId);
        konk.fs.fetchMapData(this.job.jobId, this.splitId);
    }
    MapTask.prototype.onData = function(data) {
        konk.log('MapTask onData() :' + this.splitId);
        var m = konk.createMessage(konk.TYPE_DO, {
            action: "map",
            job: this.job,
            splitId: this.splitId,
            code: "function map(k, data, emit) { var ws = data.split(/\\b/); for (var w in ws) { emit(ws[w], 1); } }",
            data: data,
        });
        konk.engine.sendMessage(m);
    }
    MapTask.prototype.done = function(result) {
        konk.log('MapTask done() :' + this.splitId);
        this.result = result;
        konk.map.notifyTaskDone(this)
    }

    MapTaskFactory = function() {}
    /* NOTE: this is a kind of 'static' function */
    MapTaskFactory.createInstance = function(job, splitId) {
        return new konk.MapTask(job, splitId);
    }
    

    /* ReduceTask class */
    function ReduceTask(job, partitionId) {
        this.splits = [];
        this._i = -1;

        this.job = job;
        this.partitionId = partitionId;

        if (job) {
            for (var i=0; i<this.job.M; i++) {
                this.splits[i] = null;
            }
        }
    }
    ReduceTask.prototype.start = function() {
        konk.log('ReduceTask start() :' + this.partitionId);
        this.doSplit();
    }
    ReduceTask.prototype.getNextSplitId = function() {
        //[TODO: make this non-linear / random]
        if (this._i < this.job.M) {
            this._i += 1;
            return this._i;
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
            konk.fs.writeOutputFile(this.job.jobId, this.partitionId);
            return;
        }
        konk.reduce.nextSplit(this, this.getNextSplitId());
    }
    ReduceTask.prototype.onSave = function(res) {
        konk.log("reduceTask onSave() :" + res);
        this.done();
    }
    ReduceTask.prototype.done = function() {
        konk.log('ReduceTask done() :' + this.partitionId);
        konk.reduce.notifyTaskDone(this)
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
        return new konk.ReduceTask(job, partitionId);
    }

    /* ReduceSplit class */
    function ReduceSplit(job, partitionId, splitId, locations) {
        this.job = job;
        this.partitionId = partitionId;
        this.splitId = splitId;
        this.locations = locations;
        this._done = false;
        this.locationPtr = -1;

        this.result = null;
    }
    ReduceSplit.prototype.start = function() {
        konk.log('ReduceSplit start() :' + this.partitionId + ',' + this.splitId);
        this.fetch();
    }
    ReduceSplit.prototype.fetch = function() {
        konk.log('ReduceSplit fetch() :' + this.partitionId + ',' + this.splitId + ',' + this.locationPtr);
        this.locationPtr++;
        if (this.locationPtr < this.locations.length) {
            konk.p2p.fetchIntermediateData(this.job.jobId, this.partitionId, this.splitId, this.locations[this.locationPtr]);
        }
    }
    ReduceSplit.prototype.onData = function(data) {
        konk.log('reduceSplit onData() :' + this.partitionId + ',' + this.splitId);
        var m = konk.createMessage(konk.TYPE_ENG, {
            action: 'reduce',
            jobId: this.job.jobId,
            partitionId: this.partitionId,
            splitId: this.splitId,
            data: data,
            code: "function reduce(k, vs, emit) { var tot = 0; for (var v in vs) { tot += vs[v]; } emit(tot); }"
        });
        konk.engine.sendMessage(m);
    }
    ReduceSplit.prototype.onDataError = function() {
        konk.log('ReduceSplit onDataError() :' + this.partitionId + ',' + this.splitId);
        this.fetch();
    }
    ReduceSplit.prototype.done = function(result) {
        konk.log('ReduceSplit done() :' + this.partitionId + ',' + this.splitId);
        this.result = result;
        this._done = true;
        konk.reduce.notifySplitDone(this);
    }
    ReduceSplit.prototype.isDone = function() {
        return this._done;
    }

    ReduceSplitFactory = function() {}
    /* NOTE: this is a kind of 'static' function */
    ReduceSplitFactory.createInstance = function(job, partitionId, splitId, locations) {
        return new konk.ReduceSplit(job, partitionId, splitId, locations);
    }

    return {
        DEBUG: DEBUG,
        MASTER_WS_URL: MASTER_WS_URL,

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
            if (!konk._autostart) {
                konk.log('No autoload. aborting init.');
                konk._autostart = true;
                return;
            }

            konk.status = STATUS_INIT;

            if (("Worker" in window) == false) {
                konk.setStatus(
                    STATUS_ERROR,
                    'Your browser does not seem to support webworkers. Aborting.'
                );
                return;
            }
            if (("WebSocket" in window) == false) {
                konk.setStatus(
                    STATUS_ERROR,
                    'Your browser does not seem to support websockets. Aborting.'
                );
                return;
            }
            if (("BrowserSocket" in window) == false) {
                konk.setStatus(
                    STATUS_ERROR,
                    'Your browser does not seem to support browsersockets. Aborting.'
                );
                return;
            }

            // create a engine thread
            try {
                konk.engine.init();
            }
            catch (ex) {
                konk.setStatus(
                    STATUS_ERROR,
                    'Could not create engine thread: ' + + ex
                );
                return;
            }

            // make a ws connection to the master
            try {
                konk.master.init();
            }
            catch (ex) {
                konk.setStatus(
                    STATUS_ERROR,
                    'Could not connect to master: ' + MASTER_WS_URL + ': ' + ex
                );
                return;
            }

            // open a bs listener
            try {
                konk.server.init();
            }
            catch (ex) {
                konk.setStatus(
                    STATUS_ERROR,
                    'Could not open browsersocket: ' + ex
                );
            }

        },
        start: function() {
            // use this if _autostart is false
            konk.heartbeat.start();
            konk.master.greeting();
        },
        step: function() {
            var m = konk.createMessage(konk.TYPE_CTL, {
                action: 'step'
            });
            konk.engine.sendMessage(m);
        },
        idle: function(msg) {
            konk.active.status = 'idle';
            konk.active.jobId = null;
            konk.active.task = null;
            konk.active.tid = null;

            konk.log('In idle state');
        },
        stop: function() {
            konk.master.stop();
            konk.engine.stop();
            konk.server.stop();
            konk.heartbeat.stop();
            konk.setStatus(
                STATUS_STOPPED,
                'stopped'
            );
        },

        /*
            web worker thread that does the actual work */
        engine: {
            thread: null,

            init: function() {
                konk.engine.thread = new Worker('worker-engine.js');
                konk.engine.thread.onmessage = konk.engine.onmessage;
            },
            stop: function() {
                konk.engine.thread.terminate();
            },
            onmessage: function(msg) {
                if (msg.data.type == konk.TYPE_ENG) {
                    switch (msg.data.payload.action) {
                        case 'map':
                            konk.map.tasks[msg.data.payload.splitId].done(msg.data.payload.data); 
                            break;
                        case 'reduce':
                            konk.reduce.tasks[msg.data.payload.partitionId].splits[msg.data.payload.splitId].done(msg.data.payload.data); 
                            break;
                        default:    
                            // swallow for now
                    }
                    konk.log(msg.data, 'e');
                }
                else if (msg.data.type == konk.TYPE_LOG) {
                    konk.log(msg.data.payload.message, 'log');
                }
            },
            sendMessage: function(msg) {
                konk.engine.thread.postMessage(msg);
                konk.log(msg, 'we');
            }
        },

        /*
            websockets connection to the master server */
        master: {
            /* the main ws channel to the master */
            ws: null,

            init: function() {
                /*[TODO: should we have a protocol here?] */
                konk.master.ws = new WebSocket(MASTER_WS_URL, "worker");
                konk.master.ws.onopen = konk.master.onopen;
                konk.master.ws.onclose = konk.master.onclose;
                konk.master.ws.onerror = konk.master.onerror;
            },
            stop: function() {
                konk.master.ws.close();
            },
            greeting: function() {
                try {
                    // send the 'socket' message to master
                    var m = konk.createMessage(konk.TYPE_ACK, {
                        action: 'socket',
                        protocol: 'ws',
                        port: konk.server.bs.port,
                        resource: konk.server.bs.resourcePrefix
                    });
                    konk.master.sendMessage(m);
                }
                catch (ex) {
                    konk.setStatus(
                        STATUS_ERROR,
                        'Could not send greeting to master: ' + ex
                    );
                }

                // start accepting server messages from the master
                konk.master.ws.onmessage = konk.master.onmessage;
            },

            /* 
                if we receive a message from the master,
                forward it to the engine thread */
            onmessage: function(e) {
                var msg = konk.readMessage(e.data);
                if (msg.type == konk.TYPE_DO) {
                    switch(msg.payload.action) {
                        case 'mapTask':
                            konk.map.startTask(msg.payload);
                            break;
                        case 'reduceTask':
                            konk.reduce.startTask(msg.payload);
                            break;
                        case 'reduceSplit':
                            konk.reduce.startSplit(msg.payload);
                            break;
                        case 'idle':
                            konk.idle(msg.payload);
                            break;
                        default:    
                            // swallow for now
                    }
                }
                else if (msg.type == konk.TYPE_P2P) {
                    //worker.reduce.startUploaded(msg.payload);
                    konk.p2p.exec(msg.payload);
                }
                else if (msg.type == konk.TYPE_CTL) {
                    konk.control.exec(msg.payload);
                }
                else if (msg.type == konk.TYPE_FS) {
                    konk.fs.exec(msg.payload);
                }
                konk.log(msg, 'm');
            },
            onopen: function(e) { 
                if (!konk._autostart) {
                    konk.master.greeting();
                }

                // successful init
                konk.setStatus(STATUS_IDLE, 'init complete');
            },
            onclose: function(e) {
                /* FIXME: why does this cause an error? */
                konk.log('ws:close');
                konk.stop();   
            },
            onerror: function(e) {
                /*[TODO]*/
                konk.log('ws:error');
            },
            sendMessage: function(msg) {
                konk.master.sendMessageExec(msg, false);
            },
            sendMessageExec: function(msg, _no_reset_hb) {
                konk.master.ws.send(JSON.stringify(msg));
                konk.log(msg, 'w');
                if (!_no_reset_hb) {
                    konk.heartbeat.reset();
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
                clearInterval(konk.heartbeat.tid);
                konk.heartbeat.tid = setInterval(konk.heartbeat.heartbeat, konk.heartbeat.HB_INTERVAL_MS);
                konk.log('Heartbeat started');
            },
            heartbeat: function() {
                var m = konk.createMessage(konk.TYPE_HB, {
                    action: konk.active.status,
                    jobId: konk.active.jobId
                });
                konk.master.sendMessageExec(m, true);
            },
            reset: function() {
                konk.heartbeat.stop();
                konk.heartbeat.start();
            },
            stop: function() {
                clearInterval(konk.heartbeat.tid);
                konk.log('Heartbeat stopped');
            }
        },

        /*
            map tasks */
        map: {
            tasks: [],

            startTask: function(spec) {
                /*[FIXME: check for job/split safety]*/
                var t = konk.MapTaskFactory.createInstance(spec.job, spec.mapStatus.splitId, 'FIXME_DATASRC');
                konk.map.tasks[t.splitId] = t;
                konk.map.tasks[t.splitId].start();
                konk.active.jobId = t.job.jobId;
                konk.active.task = t;
                konk.active.status = 'mapTask';
            },
            notifyTaskDone: function(t) {
                var m = konk.createMessage(konk.TYPE_ACK, {
                    action: 'mapTask',
                    mapStatus: {
                        splitId: t.splitId
                    },
                    reduceStatus: null, //TODO
                    jobId: t.job.jobId
                });
                konk.master.sendMessage(m);
            }
        },

        reduce: {
            //FIXME: there should only be one reduce task at any one time?
            tasks: [],

            startTask: function(spec) {
                var t = konk.ReduceTaskFactory.createInstance(spec.job, spec.reduceStatus.partitionId);
                konk.reduce.tasks[t.partitionId] = t;
                konk.reduce.tasks[t.partitionId].start();
                konk.active.jobId = t.job.jobId;
                konk.active.task = t;
                konk.active.status = 'reduceTask';
            },
            startSplit: function(spec) {
                var t = konk.ReduceSplitFactory.createInstance(spec.job, spec.reduceStatus.partitionId, spec.reduceStatus.splitId, spec.reduceStatus.locations);
                konk.reduce.tasks[t.partitionId].splits[t.splitId] = t;
                konk.reduce.tasks[t.partitionId].splits[t.splitId].start();
                konk.active.jobId = t.job.jobId;
                konk.active.task = t;
                konk.active.status = 'reduceTask';
            },
            nextSplit: function(t, splitId) {
                var m = konk.createMessage(konk.TYPE_ACK, {
                    action: 'reduceSplit',
                    reduceStatus: {
                        partitionId: t.partitionId,
                        splitId: splitId
                    },
                    unreachable: [
                    ],
                    jobId:t.job.jobId});
                konk.master.sendMessage(m);
            },
            startUploaded: function(spec) {
                //[TODO: also pos. rename]
            },
            notifySplitDone: function(t) {
                konk.reduce.tasks[t.partitionId].doSplit();
            },
            notifyTaskDone: function(t) {
                var m = konk.createMessage(konk.TYPE_ACK, {
                    action: 'reduceTask',
                    reduceStatus: {
                        partitionId: t.partitionId
                    },
                    unreachable: [
                    ],
                    jobId: t.job.jobId
                });
                konk.master.sendMessage(m);
            }
        },

        /*
            interaction with the file system */
        fs: {
            FS_BASE_URI: 'http://localhost:8080/fs/',

            writeOutputFile: function(jobId, partitionId) {
                // connect to fs and upload partition
                var req = new XMLHttpRequest();  
                req.open('POST', konk.fs.getPartitionUrl(jobId, partitionId), true);  
                req.onreadystatechange = function(aEvt) {  
                    if (req.readyState == 4) {  
                        if (req.status == 200) {
                            if (typeof(konk.reduce.tasks[partitionId].onSave) == 'function') {
                                konk.reduce.tasks[partitionId].onSave(req.responseText);
                            }
                        }
                        else {
                            konk.log('FAILED: ' + req.status);
                        }
                    }  
                };
                req.overrideMimeType('text/plain');
                req.send(konk.reduce.tasks[partitionId].getDataJSON());
            },

            fetchMapData: function(jobId, splitId) {
                // connect to fs and retrieve split
                var req = new XMLHttpRequest();  
                req.open('GET', konk.fs.getSplitUrl(jobId, splitId), true);  
                req.onreadystatechange = function(aEvt) {  
                    if (req.readyState == 4) {  
                        if (req.status == 200) {
                            if (typeof(konk.map.tasks[splitId].onData) == 'function') {
                                konk.map.tasks[splitId].onData(req.responseText);
                            }
                        }
                        else {
                            konk.log('FAILED: ' + req.status);
                        }
                    }  
                };
                req.send(null);  
            },
            getPartitionUrl: function(jobId, partitionId) {
                konk.log(konk.fs.FS_BASE_URI + 'partitions/' + jobId + '/' + partitionId + '/');
                return konk.fs.FS_BASE_URI + 'partitions/' + jobId + '/' + partitionId + '/';
            },
            getSplitUrl: function(jobId, splitId) {
                konk.log(konk.fs.FS_BASE_URI + 'splits/' + jobId + '/' + splitId + '/');
                return konk.fs.FS_BASE_URI + 'splits/' + jobId + '/' + splitId + '/';
            }
        },

        /*
            interaction with other worker nodes via browsersockets */
        p2p: {
            fetchIntermediateData: function(jobId, partitionId, splitId, location) {
                konk.log('p2p.fetchIntermediateData() :' + partitionId + ',' + splitId + ',' + location);

                var msg = null;
                if (konk.server.isLocal(location)) {
                    var data = konk.map.tasks[splitId].result[partitionId];
                    konk.reduce.tasks[partitionId].splits[splitId].onData(data);
                }
                else {
                    // go over network to get the data
                    var ws = new WebSocket(location);
                    ws.onopen = function() {
                        var m = konk.createMessage(konk.TYPE_P2P, {
                            action: 'download',
                            jobId: jobId,
                            partitionId: partitionId,
                            splitId: splitId,
                            location: location
                        });
                        this.send(JSON.stringify(m));
                    }
                    ws.onmessage = function(e) {
                        msg = konk.readMessage(e.data);
                        konk.log(e.data);
                    }
                    ws.onclose = function() {
                        if (msg) {
                            if (msg.payload.action == 'upload') {
                                if (typeof(konk.reduce.tasks[msg.payload.partitionId].splits[msg.payload.splitId].onData) == 'function') {
                                    konk.reduce.tasks[msg.payload.partitionId].splits[msg.payload.splitId].onData(msg.payload.data);
                                }
                            }
                        }
                    }
                    ws.onerror = function() {
                        konk.reduce.tasks[msg.payload.partitionId].splits[msg.payload.splitId].onDataError();
                    }
                }
            }
        },

        /*
            browsersocket connection to other konk nodes */
        server: {
            /* the browsersocket for communicating with other worker nodes */
            bs: null,

            init: function() {
                konk.server.bs = new BrowserSocket(konk.server.handlerFactory);
                konk.server.bs.onerror = konk.server.onerror;
            },
            stop: function() {
                konk.server.bs.stop();
            },

            handlerFactory: function(req) {
                return new konk.server.handler(req);
            },
            handler: function(req) {
                this.onmessage = function(e) {
                    /*[TODO]*/
                    var msg = konk.readMessage(e.data);
                    if (msg.type == konk.TYPE_P2P) {
                        switch (msg.payload.action) {
                            case 'download':
                                // send back the partition/split result
                                var m = konk.createMessage(konk.TYPE_P2P, {
                                    action: 'upload',
                                    jobId: msg.payload.jobId,
                                    partitionId: msg.payload.partitionId,
                                    splitId: msg.payload.splitId,
                                    data: konk.map.tasks[msg.payload.splitId].result[msg.payload.partitionId]
                                });
                                this.send(JSON.stringify(m));
                                break;
                            default:
                                // swallow for now
                        }
                    }
                    konk.log('bs:handler:onmessage: ' + e.data);
                    this.close();
                }
                this.onopen = function(e) { 
                    /*[TODO]*/
                    konk.log('bs:handler:open');
                }
                this.onclose = function(e) {
                    /*[TODO]*/
                    konk.log('bs:handler:close');
                }
                this.onerror = function(e) {
                    /*[TODO]*/
                    konk.log('bs:handler:error');
                }
            },
            onerror: function(e) {
                /*[TODO]*/
                konk.log('bs:error');
            },
            isLocal: function(addr) {
                return (addr.search(konk.server.bs.resourcePrefix) != -1);
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
                konk.log(msg);
            }
            konk.status = status;
        },
        util: {
            esc: function(s) {
                return s.replace(/&/g, '&amp;')
                        .replace(/</g, '&lt;')
                        .replace(/>/g, '&gt;');
            }
        },
        log: function(s, level) {
            if (konk.DEBUG) {
                if (typeof(console) != 'undefined' && typeof(console.log) == 'function') {
                    console.log(s);
                }
            }
        }
    }
})();
window.addEventListener('load', konk.init, false);

function _gP(o) {
    var s = '[';
    for (var p in o) {
        s += p + '->' + o[p] + ', ';
    }
    return (s + ']');
}

