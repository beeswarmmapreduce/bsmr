function Job(description, worker) {
    this.peerId = null;
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
    this.completedChunks;
    this.unreachable = [];
}

Job.prototype.markUnreachable = function(url) {
    this.unreachable.push(url);
}

Job.prototype._nextChunkID = function(partitionId) {
    for (var i = 0; i < this.M; i++) {
        if (!this.completedChunks[i]) {
            return i;
        }
    }
}

// events from worker

Job.prototype.onMap = function(splitId) {
    this.input.feed(splitId, this.mengine);
}

Job.prototype.onReduceBucket = function(bucketId) {
    this.completedChunks = [];
    this.rengine.reset();
    this.unreachable = [];
    this.worker.reduceChunk(this._nextChunkID(), bucketId, this.unreachable);
}

Job.prototype.onReduceChunk = function(splitId, bucketId, someUrls) {
    this.unreachable = [];
    this.iengine.feed(splitId, bucketId, someUrls, this.rengine)
}

//events from inter

Job.prototype.setOwnPeerId = function(url) {
	this.peerId = url;
	console.log("Job::setOwnPeerId() "+this.peerId);
	this.worker.hb();
}

Job.prototype.onChunkFail = function(splitId, bucketId) {
    this.worker.reduceChunk(this._nextChunkID(), bucketId, this.unreachable);
}

//events from iengine

Job.prototype.onMapComplete = function(splitId) {
    this.worker.mapComplete(splitId)
}

//events from rengine

Job.prototype.onChunkComplete = function(splitId, bucketId) {
    this.completedChunks[splitId] = true;
    var nextId = this._nextChunkID();
    if (nextId) {
        this.worker.reduceChunk(nextId, bucketId, this.unreachable);
    } else {
        this.rengine.saveResults(bucketId);
    }
}

//events from output

Job.prototype.onBucketComplete = function(bucketId) {
    this.worker.bucketComplete(bucketId);
}

