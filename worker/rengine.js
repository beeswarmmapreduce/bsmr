function Rengine(reducer, output, job, bucketId) {
  this.output = output;
  this.bucketId = bucketId;
  this.job = job;
  this.buffer = [];
  this.rtask = new Rtask(reducer);
  this.chunkreg = new ChunkRegistrar(this.job.M);
  this._nextChunk(bucketId);
}

Rengine.prototype._nextChunk = function(bucketId) {
    if (this.chunkreg.allDone()) {
        this.rtask.feed(bucketId, this.output);
    } else {
    	var nextId = this.chunkreg.nextChunkID();
    	this.job.suggestChunk(nextId, bucketId);
    }
}

Rengine.prototype.onChunkFail = function(splitId, bucketId) {
	this.buffer = [];
    this._nextChunk(bucketId);
}

// events from iengine

Rengine.prototype.write = function(splitId, bucketId, pairs, more) {
	this.buffer.push(pairs);
    if (!more) {
    	for(var i in this.buffer) {
    		var frame = this.buffer[i];
    		this.rtask.reduceSome(frame);
    	}
    	this.buffer = [];
        this.chunkreg.markDone(splitId);
        this._nextChunk(bucketId);
    }
}
