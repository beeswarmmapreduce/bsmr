/**
 worker.js
 BSMR client-side worker node implementation
 This runs in a Web Worker context.
 
 Konrad Markus <konker@gmail.com>

 TODO:
    - heartbeat
        - reset timer when other message sent
        - block clash of message/heartbeat?


    - node-node comms (bs)

    - storage of map intermediate results in bsmr
    - storage of reduce results in bsmr
        - bsmr should save results to fs

    - code/data!
        - how should these look?
            - MAP/REDUCE:
                given:
                ---
                function map(data) {
                    //var v2 = do_something(v1);
                    for (var w in data.split()) {
                        emit(w, 1);
                    }
                }
                function reduce(k, vs) {
                    var tot = 0;
                    for (var v in vs) {
                        tot += vs[v];
                    }
                    emit(tot);
                }
                --

                var partitions = new Array(R);
                for (var i=0; i<R; i++) {
                    partitions[i] = {};
                }
                function __exec_map(splitId) {
                    data = fs_fetch_split(splitId);
                    (function() {
                        var emit = _map_emit;
                        map(data);
                    })();
                }

                _exec_reduce(partitionId) {
                    var ks = _get_all_keys_in_partition(partitionId);

                    for (var k in ks) {
                        (function() {
                            var emit = function(v2) { _reduce_emit(ks[k], v2); };
                            reduce(ks[k], _get_all_values_for(partitionId, ks[k]));
                        })();
                    }
                }

                _get_all_keys_in_partition(partitionId) {
                    var ret = [];
                    for (var k in partitions[partitionId]) {
                        ret.push(k);
                    }
                    return ret;
                }

                _get_all_values_for(partitionId, key) {
                    return partitions[partitionId][key];
                }

                function hash(k) {
                    return (some_kind_of_hash(k) % R);
                }
                function _map_emit(k, v) {
                    if (typeof(partitions[hash(k)][k]) == 'undefined') {
                        partitions[hash(k)][k] = [];
                    }
                    partitions[hash(k)][k].push(v);
                }
                function _reduce_emit(k, v2) {
                    results[k] = v2;
                }

        
 */

var worker = (function() {
    var DEFAULT_TASK_LEN_MS = 1000;

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
        this._i = -1;

        this.job = job;
        this.partitionId = partitionId;
        this.code = job && eval(job.code);

        if (job) {
            for (var i=0; i<this.job.M; i++) {
                this.splits[i] = null;
            }
        }
    }
    ReduceTask.prototype.start = function() {
        worker.log('Reduce task started');
        this.doSplit();
    }
    ReduceTask.prototype.getNextSplitId = function() {
        //[TODO: make this non-linear and random]
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
            this.done();
            return;
        }
        worker.reduce.nextSplit(this, this.getNextSplitId());
    }
    ReduceTask.prototype.done = function() {
        worker.log('Reduce task done');
        worker.reduce.notifyTaskDone(this)
    }

    ReduceTaskFactory = function() {}
    /* NOTE: this is a kind of 'static' function */
    ReduceTaskFactory.createInstance = function(job, partitionId) {
        return new worker.ReduceTask(job, partitionId);
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
        worker.log('ReduceSplit started');
        // for l in location:
        //      worker.fetchMapData(this, l);
        // ...?
        // this.done()
    }
    ReduceSplit.prototype.isDone = function() {
        return this._done;
    }
    ReduceSplit.prototype.done = function() {
        worker.log('ReduceSplit done');
        this._done = true;
        worker.reduce.notifySplitDone(this);
    }

    ReduceSplitFactory = function() {}
    /* NOTE: this is a kind of 'static' function */
    ReduceSplitFactory.createInstance = function(job, partitionId, splitId, location) {
        return new worker.ReduceSplit(job, partitionId, splitId, location);
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

        ReduceTask: ReduceTask,
        ReduceTaskFactory: ReduceTaskFactory,

        ReduceSplit: ReduceSplit,
        ReduceSplitFactory: ReduceSplitFactory,

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
                if (worker.active.task) {
                    if (worker.active.task instanceof worker.ReduceTask) {
                        worker.active.task.doSplit();
                    }
                    else {
                        worker.active.task.done();
                    }
                }
                else {
                    worker.log('Nothing to do: Idle');
                }
            },
            stop: function() {
                //[TODO?]
                clearTimeout(worker.active.tid);
                clearTimeout(worker.heartbeat.tid);
            },
            idle: function() {
                worker.active.status = 'idle';
                worker.active.jobId = null;
                worker.active.task = null;
                worker.active.tid = null;

                worker.log('In idle state');
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
            HB_INTERVAL_MS: 10000,
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
                var t = worker.MapTaskFactory.createInstance(spec.job, spec.mapStatus.splitId, 'FIXME_DATASRC');
                worker.map.tasks[t._id] = t;
                worker.map.tasks[t._id].start();
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
                worker.sendMessage(m);
            }
        },

        reduce: {
            //FIXME: there should only be one reduce task at any one time?
            tasks: [],

            startTask: function(spec) {
                //var t = new ReduceTask(spec.job, spec.reduceStatus.partitionId);
                var t = worker.ReduceTaskFactory.createInstance(spec.job, spec.reduceStatus.partitionId);
                worker.reduce.tasks[t.partitionId] = t;
                worker.reduce.tasks[t.partitionId].start();
                worker.active.jobId = t.job.jobId;
                worker.active.task = t;
                worker.active.status = 'reduceTask';
            },
            startSplit: function(spec) {
                //var t = new ReduceSplit(spec.job, spec.reduceStatus.partitionId, spec.reduceStatus.splitId, spec.location);
                var t = worker.ReduceSplitFactory.createInstance(spec.job, spec.reduceStatus.partitionId, spec.reduceStatus.splitId, spec.location);
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
                    unreachable: [
                    ],
                    jobId:t.job.jobId});
                worker.sendMessage(m);
            },
            startUploaded: function(spec) {
                //[TODO: also pos. rename]
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
                    unreachable: [
                    ],
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
                worker.control.idle(msg.data.payload);
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

