function Job(description, worker) {
    this.id = description.jobId;
    this.R = description.R;
    this.M = description.M;
    this.output = description.output(this);
    this.rengine = new Rengine(description.reducer, this.output, this);
    this.iengine = new Iengine(this.R, this, description.inter, description.chooseBucket);
    var mengineout = this.iengine;
    if (description.combiner) {
        this.cengine = new Cengine(description.combiner, this.iengine);
        mengineout = this.cengine;
    }
    this.mengine = new Mengine(description.mapper, mengineout);
    this.input = description.input(this.M);
    this.worker = worker;
    this.splits;
    this.unreachable = [];
}

Job.prototype.markUnreachable = function(url) {
    this.unreachable.push(url);
}

Job.prototype._nextSplit = function(partitionId) {
    for (var i = 0; i < this.M; i++) {
        if (!this.splits[i]) {
            return i;
        }
    }
}

// events from worker

Job.prototype.onMap = function(splitId) {
    this.input.feed(splitId, this.mengine);
}

Job.prototype.onReduceTask = function(partitionId) {
    this.splits = [];
    this.rengine.reset();
    this.unreachable = [];
    this.worker.reduceSplit(this._nextSplit(), partitionId, this.unreachable);
}

//events from inter

Job.prototype.onSplitFail = function(splitId, partitionId) {
    this.worker.reduceSplit(this._nextSplit(), partitionId, this.unreachable);
}

//events from iengine

Job.prototype.onMapComplete = function(splitId) {
    this.worker.mapComplete(splitId)
}

Job.prototype.onReduceSplit = function(splitId, partitionId, someUrls) {
    this.unreachable = [];
    this.iengine.feed(splitId, partitionId, someUrls, this.rengine)
}

//events from rengine

Job.prototype.onSplitComplete = function(splitId, partitionId) {
    this.splits[splitId] = true;
    var nextId = this._nextSplit();
    if (nextId) {
        this.worker.reduceSplit(nextId, partitionId, this.unreachable);
    } else {
        this.rengine.saveResults(partitionId);
    }
}

//events from output

Job.prototype.onPartitionComplete = function(partitionId) {
    this.worker.partitionComplete(partitionId);
}

