function Job(description, worker) {
    this.id = description.jobId;
    this.R = description.R;
    this.M = description.M;
    this.output = new Output();
    this.rengine = new Rengine(description.reducer, this.output);
    this.inter = new Intermediary(this.R, this.rengine, this);
    this.mengine = new Mengine(description.mapper, this.inter);
    this.input = new Input(this.M, this.mengine);
    this.partitions = {};
    this.worker = worker;
}

Job.prototype.onMap = function(splitId) {
    this.inter.startWrite(splitId);
    this.input.start(splitId);
}

Job.prototype.onMapComplete = function(splitId) {
    this.worker.mapComplete(splitId)
}

Job.prototype.onReduceTask = function(partitionId) {
    this.worker.acceptReduceTask(partitionId);
}


//TODO: could be rubbish
Job.prototype._nextSplit = function(partitionId) {
    var state = this.partitions[partitionId];
    for (var i = 0; i < this.M; i++) {
        if (!state[i]) {
            return i;
        }
    }
}

//TODO: could be rubbish
Job.prototype.onReduceSplit = function(partitionId, splitId) {
    if (! this.partitions[partitionId]) {
        this.partitions[partitionId] = [];
    }
    this.rengine.reduce(partitionId, splitId);
    this.partitions[partitionId][splitId] = true;
    var nextId = this._nextSplit(partitionId);
    if (nextId) {
        next(nextId);
    } else {
        this.output.write(partitionId);
        complete();
    }

}
