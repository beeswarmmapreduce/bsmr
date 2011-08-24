function Job(description, worker) {
    this.id = description.jobId;
    this.R = description.R;
    this.M = description.M;
    this.output = new Output(this);
    this.rengine = new Rengine(description.reducer, this.output, this);
    this.inter = new Intermediary(this.R, this.rengine, this);
    this.mengine = new Mengine(description.mapper, this.inter);
    this.input = new Input(this.M, this.mengine);
    this.partition;
    this.splits = [];
    this.worker = worker;
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
    this.inter.startWrite(splitId);
    this.input.start(splitId);
}

Job.prototype.onReduceTask = function(partitionId) {
    this.partition = partitionId;
    this.rengine.reset();
    this.worker.reduceSplit(partitionId, this._nextSplit());
}

//events from inter

Job.prototype.onMapComplete = function(splitId) {
    this.worker.mapComplete(splitId)
}

Job.prototype.onReduceSplit = function(partitionId, splitId) {
    this.output.startWrite(partitionId);
    this.inter.start(partitionId, splitId)
}

//events from rengine

Job.prototype.onSplitComplete = function(splitId) {
    this.splits[splitId] = true;
    var nextId = this._nextSplit();
    if (nextId) {
        this.worker.reduceSplit(this.partition, nextId);
    } else {
        this.rengine.saveResults(this.partition);
    }
}

//events from output

Job.prototype.onPartitionComplete = function() {
    console.log(this.partition + " complete");
}

