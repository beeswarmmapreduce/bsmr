function Job(description, worker) {
	this.worker = worker;
    this.peerId = null;
    this.unreachable = [];
    this.id = description.jobId;
    this.R = description.R;
    this.M = description.M;
    this.mapper = description.mapper;
    this.reducer = description.reducer;
    this.combiner = description.combiner;    
    this.mengine;
    this.rengine;
    this.cengine;
    this.input = description.input(this.M);
    this.iengine = new Iengine(this.R, this, description.inter, description.chooseBucket);
    this.output = description.output(this);
}

// events from worker

Job.prototype.onMap = function(splitId) {
	this.rengine = undefined;
    var mengineout = this.iengine;
    if (this.combiner) {
        this.cengine = new Cengine(this.combiner, this.iengine);
        mengineout = this.cengine;
    }
    this.mengine = new Mengine(this.mapper, mengineout);
    this.input.feed(splitId, this.mengine);
}

Job.prototype.onReduceBucket = function(bucketId) {
	this.cengine = undefined;
	this.mengine = undefined;
	this.rengine = new Rengine(this.reducer, this.output, this, bucketId);
}

Job.prototype.onReduceChunk = function(splitId, bucketId, someUrls) {
	//filter messages related to obsolete reduce operations
	if (typeof(this.rengine) == typeof(undefined)) {
		return;
	}
	if (bucketId != this.rengine.bucketId) {
		return;
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
	if(typeof(this.rengine) != typeof(undefined)) {
		this.rengine.onChunkFail(splitId, bucketId);
	}
}

//events from iengine

Job.prototype.onMapComplete = function(splitId) {
	this.cengine = undefined;
	this.mengine = undefined;
    this.worker.mapComplete(splitId)
}

//events from rengine

Job.prototype.suggestChunk = function(splitId, bucketId) {
    var broken = this.unreachable;
    //this.unreachable = [];
	this.worker.suggestChunk(splitId, bucketId, broken);
}

//events from output

Job.prototype.onBucketComplete = function(bucketId) {
	this.rengine = undefined;
    this.worker.bucketComplete(bucketId);
}

