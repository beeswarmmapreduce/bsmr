/**
 worker.js
 BSMR client-side worker node implementation
 This runs in a Web Worker context.
 
 Konrad Markus <konker@gmail.com>

 TODO:

 */

var worker = (function() {
    var DEFAULT_TASK_LEN_MS = 2000;

    /* modes (FIXME: duplication...) */
    var MODE_NOR = 1; // normal mode
    var MODE_TMO = 2; // dummy setTimeout mode
    var MODE_IVE = 3; // user-interactive mode

    /* MapTask class */
    function MapTask(job, splitId, datasrc) {
        this._id = (new Date()).getTime() + ':' + (Math.ceil(Math.random() * 10000));

        this.job = job;
        this.splitId = splitId;
        this.datasrc = datasrc;
        this.code = job && eval(job.code);
        this.results = {};
    }
    MapTask.prototype.start = function() {
        worker.log('Map task started');
        /* do real map */
        // while (k,v) in datasrc:
        //      this.results[k] = this.code(k,v)
        // this.done()
    }
    MapTask.prototype.done = function() {
        worker.log('Map task done');
        worker.map.notifyTaskDone(this)
    }

    MapTaskFactory = function() {}
    /* NOTE: this is a kind of 'static' function */
    MapTaskFactory.createInstance = function(job, splitId, datasrc) {
        return new worker.MapTask(job, splitId, datasrc);
    }
    

    /* ReduceTask class */
    function ReduceTask(job, partitionId) {
        this.splits = [];

        this.job = job;
        this.partitionId = partitionId;
        this.code = eval(job.code);

        for (var i=0; i<this.job.M; i++) {
            this.splits[i] = null;
        }
    }
    ReduceTask.prototype.start = function() {
        worker.log('Reduce task started');
        this.doSplit();
    }
    ReduceTask.prototype.done = function() {
        worker.log('Reduce task done');
        worker.reduce.notifyDone(this)
    }
    ReduceTask.prototype.isDone = function() {
        var n = 0; 
        for (var s in this.splits) {
            if (this.splits[i].isDone()) {
                ++n;
            }
        }
        return (n == this.job.M);
    },
    ReduceTask.prototype.doSplit = function(t) {
        var that = this;
        if (this.isDone()) {
            this.done();
        }
        else {
            worker.active.tid = setTimeout(function() {
                worker.reduce.nextSplit(that);
            }, worker.control.taskLenMs);
        }
    }

    /* ReduceSplit class */
    function ReduceSplit(job, partitionId, splitId, location) {
        this.job = job;
        this.partitionId = partitionId;
        this.splitId = splitId;
        this.location = location;
        this._done = false;

        this.results = {};
    }
    ReduceSplit.prototype.start = function() {
        worker.log('ReduceSplit task started');
        // for l in location:
        //      worker.fetchMapData(this, l);
        // ...?
        // this.done()
        var that = this;
        worker.active.tid = setTimeout(function() {
            this.done();
        }, worker.control.taskLenMs)
    }
    ReduceSplit.prototype.done = function() {
        worker.log('ReduceSplit task done');
        this._done = true;
        worker.reduce.notifySplitDone(this);
    }
    ReduceSplit.prototype.isDone = function() {
        return this.done;
    }

    return {
        _DEBUG_: true,

        /* modes (FIXME: duplication...) */
        MODE_NOR: MODE_NOR,
        MODE_TMO: MODE_TMO,
        MODE_IVE: MODE_IVE,

        /* message types (FIXME: duplication...) */
        TYPE_HB: 'HB',
        TYPE_DO: 'DO',
        TYPE_ACK: 'ACK',
        TYPE_UPL: 'UPL',
        TYPE_LOG: 'LOG',
        TYPE_CTL: 'CTL',

        /* classes */
        MapTask: MapTask,
        MapTaskFactory: MapTaskFactory,

        /* track current worker activity */
        active: {
            status: 'idle',
            jobId: null,
            task: null,
            tid: null
        },

        mapTasks: {},
        reduceTasks: {},

        control: {
            mode: MODE_IVE,
            taskLenMs: DEFAULT_TASK_LEN_MS,

            setMode: function(spec) {
                switch(spec.mode) {
                    case MODE_NOR:
                        worker.log('Mode set to MODE_NOR');
                        worker.control.mode = MODE_NOR;
                        break;
                    case MODE_TMO:
                        worker.log('Mode set to MODE_TMO');
                        worker.taskLenMs = spec.taskLenMs;
                        worker.control.mode = MODE_TMO;
                        break;
                    case MODE_IVE:
                        worker.log('Mode set to MODE_IVE');
                        worker.control.mode = MODE_IVE;
                        break;
                    default:
                        // swallow for now
                }
            },
            step: function() {
                worker.active.task.done();
            },
            stop: function() {
                //[TODO?]
                clearTimeout(worker.active.tid);
                clearTimeout(worker.heartbeat.tid);
            },
            exec: function(spec) {
                switch(spec.action) {
                    case 'mode':
                        worker.control.setMode(spec);
                        break;
                    case 'hb':
                        worker.heartbeat.heartbeat();
                        break;
                    case 'step':
                        worker.control.step();
                        break;
                    case 'stop':
                        worker.control.stop();
                        break;
                    default:
                        // swallow for now
                }
            }
        },

        heartbeat: {
            HB_INTERVAL_MS: 5000,
            tid: null,

            heartbeat: function() {
                var m = worker.createMessage(worker.TYPE_HB, {
                    action: worker.active.status,
                    jobId: worker.active.jobId
                });
                worker.sendMessage(m);
                worker.heartbeat.tid = setTimeout(worker.heartbeat.heartbeat, worker.heartbeat.HB_INTERVAL_MS);
            }
        },

        map: {
            tasks: {},

            startTask: function(spec) {
                var t = worker.MapTaskFactory.createInstance(spec.job, spec.mapStatus.splitId, 'FIXME');
                worker.map.tasks[t._id] = t;
                worker.map.tasks[t._id].start();
                worker.active.jobId = t.job.jobId;
                worker.active.task = t;
                worker.active.status = 'map';
            },
            notifyTaskDone: function(t) {
                var m = worker.createMessage(worker.TYPE_ACK, {
                    action: 'mapTask',
                    mapStatus: {
                        splitId: t.splitId + "" //FIXME: remove string cast
                    },
                    reduceStatus: null, //TODO
                    jobId: t.job.jobId
                });
                worker.sendMessage(m);
            }
        },

        reduce: {
            //FIXME: there should only be one reduce task at any one time?
            tasks: [],

            startTask: function(spec) {
                var t = new ReduceTask(spec.job, spec.reduceStatus.partitionId, 'FIXME');
                worker.reduce.tasks[t.partitionId] = t;
                worker.reduce.tasks[t.partitionId].task.start();
                worker.active.jobId = t.job.jobId;
                worker.active.task = t;
                worker.active.status = 'reduce';
            },
            startSplit: function(spec) {
                var t = new ReduceSplit(spec.job, spec.reduceStatus.partitionId, spec.reduceStatus.splitId, spec.location);
                worker.reduce.tasks[t.partitionId].splits[t.splitId] = t;
                worker.reduce.tasks[t.partitionId].splits[t.splitId].start();
                worker.active.jobId = t.job.jobId;
                worker.active.task = t;
                worker.active.status = 'reduce';
            },
            nextSplit: function(spec) {
                var m = worker.createMessage(worker.TYPE_ACK, {
                    action: 'reduceSplit',
                    reduceStatus: {
                        partitionId: t.partitionId,
                        splitId: t.splitId
                    },
                    unreachable: {
                    },
                    jobId: t.job.jobId
                });
                worker.sendMessage(m);
            },
            startUploaded: function(spec) {
                //[TODO: also pos. rename]
            },
            notifySplitDone: function(t) {
                worker.reduce.tasks[t.partitionId].splits[t.splitId].doSplit();
            },
            notifyTaskDone: function(t) {
                var m = worker.createMessage(worker.TYPE_ACK, {
                    action: 'reduceTask',
                    reduceStatus: {
                        partitionId: t.partitionId
                    },
                    unreachable: {
                    },
                    jobId: t.job.jobId
                });
                worker.sendMessage(m);
            }
        },

        /* TODO: duplicated.. */
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
        sendMessage: function(m) {
            postMessage(m);
        },
        log: function(s) {
            var m = worker.createMessage(worker.TYPE_LOG, {
                message: s
            });
            worker.sendMessage(m);
        }
    }
})();

/* receive incoming message from parent */
function onmessage(msg) {
    if (msg.data.type == worker.TYPE_DO) {
        switch(msg.data.payload.action) {
            case 'mapTask':
                worker.map.startTask(msg.data.payload);
                break;
            case 'reduceTask':
                worker.reduce.startTask(msg.data.payload);
                break;
            case 'reduceSplit':
                worker.reduce.startSplit(msg.data.payload);
                break;
            case 'idle':
                worker.startIdle(msg.data.payload);
                break;
            default:    
                // swallow for now
        }
    }
    else if (msg.data.type == worker.TYPE_UPL) {
        worker.reduce.startUploaded(msg.data.payload);
    }
    else if (msg.data.type == worker.TYPE_CTL) {
        worker.control.exec(msg.data.payload);
    }
}

if (worker._DEBUG_) {
    // FIXME: can we inject this (rather than pull it)?
    importScripts('opt/worker-debug.js');
}

