function Job(description, worker) {
    this.id = description.jobId;
    this.R = description.R;
    this.M = description.M;
    this.output = new Output(this);
    this.rengine = new Rengine(description.reducer, this.output, this);
    this.inter = new Intermediary(this.R, this.rengine, this);
    var mengineout = this.inter;
    if (description.combiner) {
        this.cengine = new Cengine(description.combiner, this.inter);
        mengineout = this.cengine;
    }
    this.mengine = new Mengine(description.mapper, mengineout);
    this.input = new Input(this.M, this.mengine);
    this.partition;
    this.worker = worker;
    this.splits;
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
    this.input.feed(splitId);
}

Job.prototype.onReduceTask = function(partitionId) {
    this.splits = [];
    this.partition = partitionId;
    this.rengine.reset();
    var brokenUrls = [];
    this.worker.reduceSplit(this._nextSplit(), partitionId, brokenUrls);
}

//events from inter

Job.prototype.onMapComplete = function(splitId) {
    this.worker.mapComplete(splitId)
}

Job.prototype.onReduceSplit = function(splitId, partitionId) {
    this.inter.feed(splitId, partitionId)
}

Job.prototype.onUnreachable = function(splitId, partitionId, brokenUrls) {
    this.worker.reduceSplit(this._nextSplit(), partitionId, brokenUrls);
}

//events from rengine

Job.prototype.onSplitComplete = function(splitId) {
    this.splits[splitId] = true;
    var nextId = this._nextSplit();
    if (nextId) {
        brokenUrls = []; //TODO: report some?
        this.worker.reduceSplit(nextId, this.partition, brokenUrls);
    } else {
        this.rengine.saveResults(this.partition);
    }
}

//events from output

Job.prototype.onPartitionComplete = function() {
    this.worker.partitionComplete(this.partition);
}

