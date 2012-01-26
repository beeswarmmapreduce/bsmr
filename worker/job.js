function Job(description, worker) {
	this.worker = worker;
    this.peerId = null;
    this.id = description.jobId;
    this.R = description.R;
    this.M = description.M;
    this.output = description.output(this);
    this.reducer = description.reducer;
    this.rengine;
    this.iengine = new Iengine(this.R, this, description.inter, description.chooseBucket);
    var mengineout = this.iengine;
    if (description.combiner) {
        this.cengine = new Cengine(description.combiner, this.iengine);
        mengineout = this.cengine;
    }
    this.mengine = new Mengine(description.mapper, mengineout);
    this.input = description.input(this.M);
    this.unreachable = [];
}

// events from worker

Job.prototype.onMap = function(splitId) {
	console.log('MAP');
	this.rengine = undefined;
    this.input.feed(splitId, this.mengine);
}

Job.prototype.onReduceBucket = function(bucketId) {
	console.log('REBU');
	this.rengine = new Rengine(this.reducer, this.output, this, bucketId);
}

Job.prototype.onReduceChunk = function(splitId, bucketId, someUrls) {
	console.log('RECHU(' + splitId + ')');
	if (typeof(this.rengine) == typeof(undefined)) {
		throw 'Received a ReduceChunk command while not reducing!';
	}
	if (bucketId != this.rengine.bucketId) {
		throw 'Received a ReduceChunk command for bucket A while reducing bucket B!';
	}
    this.iengine.feed(splitId, bucketId, someUrls, this.rengine)
}

//events from inter

Job.prototype.setOwnPeerId = function(url) {
	this.peerId = url;
	console.log("Job::setOwnPeerId() " + this.peerId);
	this.worker.hb();
}

Job.prototype.markUnreachable = function(url) {
    this.unreachable.push(url);
}

Job.prototype.onChunkFail = function(splitId, bucketId) {
	console.log('RECHU-FAIL');
	this.rengine.onChunkFail(splitId, bucketId);
}

//events from iengine

Job.prototype.onMapComplete = function(splitId) {
	console.log('MAP-COMPLETE');
    this.worker.mapComplete(splitId)
}

//events from rengine

Job.prototype.suggestChunk = function(splitId, bucketId) {
	console.log('SUGGEST-CHUNK');
    var broken = this.unreachable;
    //this.unreachable = [];
	this.worker.suggestChunk(splitId, bucketId, broken);
}

//events from output

Job.prototype.onBucketComplete = function(bucketId) {
	console.log('REBU-COMPLETE');
	this.rengine = undefined;
    this.worker.bucketComplete(bucketId);
}

