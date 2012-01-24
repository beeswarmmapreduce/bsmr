function Job(description, worker) {
	this.worker = worker;
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
    this.completedChunks;
    this.unreachable = [];
}

Job.prototype._bucketComplete = function() {
    for (var i = 0; i < this.M; i++) {
        if (!this.completedChunks[i]) {
            return false;
        }
    }
    return true;
}

Job.prototype._randInt = function(min, max) {
	return min + Math.floor(Math.random() * (max - min));
}

Job.prototype._nextChunkID = function() {
	var incomplete = []; 
    for (var i = 0; i < this.M; i++) {
        if (!this.completedChunks[i]) {
        	incomplete.push(i);
        }
    }
    var left = incomplete.length;
    if (left > 0) {
    	var i = this._randInt(0, left);
    	return incomplete[i];
    }
}

Job.prototype._nextChunk = function(bucketId) {
    var nextId = this._nextChunkID();
    this.worker.suggestChunk(nextId, bucketId, this.unreachable);
}

// events from worker

Job.prototype.onMap = function(splitId) {
	console.log('MAP');
    this.input.feed(splitId, this.mengine);
}

Job.prototype.onReduceBucket = function(bucketId) {
	console.log('REBU');
    this.completedChunks = [];
    this.rengine.reset();
    this.unreachable = [];
    this._nextChunk(bucketId);
}

Job.prototype.onReduceChunk = function(splitId, bucketId, someUrls) {
	console.log('RECHU(' + splitId + ')');
    this.unreachable = [];
    this.iengine.feed(splitId, bucketId, someUrls, this.rengine)
}

//events from inter

Job.prototype.setOwnPeerId = function(url) {
	this.peerId = url;
	console.log("Job::setOwnPeerId() "+this.peerId);
	this.worker.hb();
}

Job.prototype.markUnreachable = function(url) {
    this.unreachable.push(url);
}

Job.prototype.onChunkFail = function(splitId, bucketId) {
	console.log('RECHU-FAIL');
	this._nextChunk(bucketId);
}

//events from iengine

Job.prototype.onMapComplete = function(splitId) {
	console.log('MAP-COMPLETE');
    this.worker.mapComplete(splitId)
}

//events from rengine

Job.prototype.onChunkComplete = function(splitId, bucketId) {
	console.log('RECHU-COMPLETE');
    this.completedChunks[splitId] = true;
    var nextId = this._nextChunkID();
    if (this._bucketComplete()) {
    	this.rengine.saveResults(bucketId);
    	
    } else {
    	this._nextChunk(bucketId);
    }
}

//events from output

Job.prototype.onBucketComplete = function(bucketId) {
	console.log('REBU-COMPLETE');
    this.worker.bucketComplete(bucketId);
}

