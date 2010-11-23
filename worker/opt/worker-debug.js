/**
 worker-debug.js

 add additional optional debug facilities to worker.js
 
 Konrad Markus <konker@gmail.com>

 TODO:
    - how is this loaded?
        - is there a better way?
 */

if (typeof('worker') != 'undefined') {
    /* ----------------------------------
     * MapTask debug versions */

    /* setTimeout mode task */
    worker.TMO_MapTask = function(job, splitId, datasrc) {
        worker.MapTask.call(this, job, splitId, datasrc);
        worker.log('TMO');
    }
    worker.TMO_MapTask.prototype = new worker.MapTask();
    worker.TMO_MapTask.prototype.start = function() {
        worker.log('Map task started (TMO)');
        worker.active.tid = setTimeout(function() {
            worker.active.task.done();
        }, worker.control.taskLenMs);
    }

    /* user-interactive mode task */
    worker.IVE_MapTask = function(job, splitId, datasrc) {
        worker.MapTask.call(this, job, splitId, datasrc);
    }
    worker.IVE_MapTask.prototype = new worker.MapTask();
    worker.IVE_MapTask.prototype.start = function() {
        worker.log('Map task started (IVE)');
    }

    /* amend the factory */
    worker.MapTaskFactory.createInstance = function(job, splitId, datasrc) {
        switch (worker.control.mode) {
            case worker.MODE_IVE:
                return new worker.IVE_MapTask(job, splitId, datasrc);
            case worker.MODE_TMO:
                return new worker.TMO_MapTask(job, splitId, datasrc);
            default:
                return new worker.MapTask(job, splitId, datasrc);
        }
    }


    /* ----------------------------------
     * ReduceTask debug versions */

    /* setTimeout mode task */
    worker.TMO_ReduceTask = function(job, partitionId) {
        worker.ReduceTask.call(this, job, partitionId);
        worker.log('TMO');
    }
    worker.TMO_ReduceTask.prototype = new worker.ReduceTask();
    worker.TMO_ReduceTask.prototype.start = function() {
        worker.log('Reduce task started (TMO)');
        this.doSplit();
    }
    worker.TMO_ReduceTask.doSplit = function() {
        if (this.isDone()) {
            this.done();
        }
        else {
            var that = this;
            worker.active.tid = setTimeout(function() {
                worker.reduce.nextSplit(that);
            }, worker.control.taskLenMs);
        }
    }


    /* user-interactive mode task */
    worker.IVE_ReduceTask = function(job, partitionId) {
        worker.ReduceTask.call(this, job, partitionId);
    }
    worker.IVE_ReduceTask.prototype = new worker.ReduceTask();
    worker.IVE_ReduceTask.prototype.start = function() {
        worker.log('Reduce task started (IVE)');
    }

    /* amend the factory */
    worker.ReduceTaskFactory.createInstance = function(job, partitionId) {
        switch (worker.control.mode) {
            case worker.MODE_IVE:
                return new worker.IVE_ReduceTask(job, partitionId);
            case worker.MODE_TMO:
                return new worker.TMO_ReduceTask(job, partitionId);
            default:
                return new worker.ReduceTask(job, partitionId);
        }
    }


    /* ----------------------------------
     * ReduceSplit debug versions */

    /* setTimeout mode task */
    worker.TMO_ReduceSplit = function(job, splitId, partitionId, location) {
        worker.ReduceSplit.call(this, job, splitId, partitionId, location);
        worker.log('TMO');
    }
    worker.TMO_ReduceSplit.prototype = new worker.ReduceSplit();
    worker.TMO_ReduceSplit.prototype.start = function() {
        worker.log('ReduceSplit task started (TMO)');
        worker.active.tid = setTimeout(function() {
            worker.active.task.done();
        }, worker.control.taskLenMs);
    }

    /* user-interactive mode task */
    worker.IVE_ReduceSplit = function(job, splitId, partitionId, location) {
        worker.ReduceSplit.call(this, job, splitId, partitionId, location);
    }
    worker.IVE_ReduceSplit.prototype = new worker.ReduceSplit();
    worker.IVE_ReduceSplit.prototype.start = function() {
        worker.log('ReduceSplit task started (IVE)');
    }

    /* amend the factory */
    worker.ReduceSplitFactory.createInstance = function(job, splitId, partitionId, location) {
        switch (worker.control.mode) {
            case worker.MODE_IVE:
                return new worker.IVE_ReduceSplit(job, splitId, partitionId, location);
            case worker.MODE_TMO:
                return new worker.TMO_ReduceSplit(job, splitId, partitionId, location);
            default:
                return new worker.ReduceSplit(job, splitId, partitionId, location);
        }
    }

}

