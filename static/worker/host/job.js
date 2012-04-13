function Job(description, worker) {
	this.worker = worker;
    this.interUrl = null;
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
    this.local =  new Localstore(description.chooseBucket, this.R);
    this.iengine = new Iengine(this, description.inter);
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
};

Job.prototype.onReduceBucket = function(bucketId) {
	console.log('rb-' + bucketId);
	this.cengine = undefined;
	this.mengine = undefined;
	this.rengine = new Rengine(this.reducer, this.output, this, bucketId);
};

Job.prototype.onReduceChunk = function(splitId, bucketId, someUrls) {
	console.log('rc-' + splitId + '-' + bucketId);
	//filter messages related to obsolete reduce operations
	if (typeof(this.rengine) == typeof(undefined)) {
		return;
	}
	if (bucketId != this.rengine.bucketId) {
		return;
	}
	
	//someUrls may be empty, as we might be the only node with the data
    this.iengine.feed(splitId, bucketId, someUrls, this.rengine);
};

//events from inter

Job.prototype.setOwnInterUrl = function(url) {
	this.interUrl = url;
	console.log("Job::setOwnInterUrl() " + this.interUrl);
	this.worker.hb();
};

Job.prototype.markUnreachable = function(url) {
	//the unreachable label is only removed when we switch jobs
    this.unreachable.push(url);
};

Job.prototype.onChunkFail = function(splitId, bucketId) {
	if(typeof(this.rengine) == typeof(undefined)) {
		return;
	}
	if (bucketId != this.rengine.bucketId) {
		return;		
	}
	this.rengine.onChunkFail(splitId, bucketId);
};

//events from iengine

Job.prototype.onMapComplete = function(splitId) {
	this.cengine = undefined;
	this.mengine = undefined;
    this.worker.mapComplete(splitId);
};

//events from rengine

Job.prototype.suggestChunk = function(splitId, bucketId) {
    var broken = this.unreachable;
	this.worker.suggestChunk(splitId, bucketId, broken);
};

//events from output

Job.prototype.onBucketComplete = function(bucketId) {
	this.rengine = undefined;
    var broken = this.unreachable;
    this.worker.bucketComplete(bucketId, broken);
};

