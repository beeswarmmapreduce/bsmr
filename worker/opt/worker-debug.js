/**
 worker-debug.js

 add additional optional debug facilities to worker.js
 
 Konrad Markus <konker@gmail.com>

 TODO:
    - how is this loaded?
        - is there a better way?
 */

if (typeof('worker') != 'undefined') {
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
}

