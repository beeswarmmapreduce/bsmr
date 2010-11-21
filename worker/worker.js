/**
 worker.js
 BSMR client-side worker node implementation
 This runs in a Web Worker context.
 
 Konrad Markus <konker@gmail.com>

 */

var worker = (function() {
    var TASK_LEN_MS = 2000;

    /* message types (dulication...) */
    var TYPE_HB  = 'HB';
    var TYPE_ACK = 'ACK';
    var TYPE_LOG = 'LOG';

    /* MapTask class */
    function MapTask(job, splitId, datasrc) {
        this._id = (new Date()).getTime() + ':' + (Math.ceil(Math.random() * 10000));

        this.job = job;
        this.splitId = splitId;
        this.datasrc = datasrc;
        this.code = eval(job.code);
        this.results = {};
    }
    MapTask.prototype.start = function() {
        // while (k,v) in datasrc:
        //      this.results[k] = this.code(k,v)
        //worker.map.notifyTaskDone(this)
        worker.log('Map task started');
        var that = this;
        var tid = setTimeout(function() {
            worker.log('Map task completed');
            worker.map.notifyTaskDone(that);
        }, TASK_LEN_MS);
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
        // for i = (0 .. job.M):
        //     worker.reduce.nextSplit(partitionId, i)
        // worker.reduce.notifyDone(this)
        worker.log('Reduce task started');
        this.doSplit();
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
            worker.log('Reduce task completed');
            worker.reduce.notifyDone(this);
        }
        else {
            var tid = setTimeout(function() {
                worker.reduce.nextSplit(that);
            }, TASK_LEN_MS);
        }
    }

    /* ReduceSplit class */
    function ReduceSplit(job, partitionId, splitId, location) {
        this.job = job;
        this.partitionId = partitionId;
        this.splitId = splitId;
        this.location = location;
        this.done = false;

        this.results = {};
    }
    ReduceSplit.prototype.start = function() {
        // for l in location:
        //      worker.fetchMapData(this, l);
        // worker.reduce.notifyDone(this)
        worker.log('ReduceSplit task started');
        var that = this;
        var tid = setTimeout(function() {
            that.done = true;
            worker.log('ReduceSplit task completed');
            worker.reduce.notifySplitDone(that);
        }, TASK_LEN_MS)
    }
    ReduceSplit.prototype.isDone = function() {
        return this.done;
    }



    return {
        active: 'idle',
        currentJobId: null,

        mapTasks: {},
        reduceTasks: {},

        heartbeat: {
            HB_INTERVAL_MS: 5000,
            tid: null,

            heartbeat: function() {
                var m = worker.createMessage(TYPE_HB, {
                    action: worker.active,
                    jobId: worker.currentJobId
                });
                worker.sendMessage(m);
                worker.heartbeat.tid = setTimeout(worker.heartbeat.heartbeat, worker.heartbeat.HB_INTERVAL_MS);
            }
        },
        map: {
            tasks: {},

            startTask: function(spec) {
                var t = new MapTask(spec.job, spec.mapStatus.splitId, 'FIXME');
                worker.map.tasks[t._id] = t;
                worker.map.tasks[t._id].start();
                worker.currentJobId = t.job.jobId;
                worker.active = 'map';
            },
            notifyTaskDone: function(t) {
                var m = worker.createMessage(TYPE_ACK, {
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
                worker.currentJobId = t.job.jobId;
                worker.active = 'reduce';
            },
            startSplit: function(spec) {
                var t = new ReduceSplit(spec.job, spec.reduceStatus.partitionId, spec.reduceStatus.splitId, spec.location);
                worker.reduce.tasks[t.partitionId].splits[t.splitId] = t;
                worker.reduce.tasks[t.partitionId].splits[t.splitId].start();
                worker.currentJobId = t.job.jobId;
                worker.active = 'reduce';
            },
            nextSplit: function(spec) {
                var m = worker.createMessage(TYPE_ACK, {
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
                var m = worker.createMessage(TYPE_ACK, {
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
            var m = worker.createMessage(TYPE_LOG, {
                message: s
            });
            worker.sendMessage(m);
        }
    }
})();

/* receive incoming message from parent */
function onmessage(msg) {
    if (msg.data.type == 'DO') {
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
    else if (msg.data.type == 'UPLOAD') {
        worker.reduce.startUploaded(msg.data.payload);
    }
}

/* kick off the heartbeat */
worker.heartbeat.heartbeat();

