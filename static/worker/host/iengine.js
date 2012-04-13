
function Iengine(job, inter, chooseBucket) {
    this.job = job;
    this.local = job.local;
    this.inter = inter(job);
    this.chooseBucket = chooseBucket;
}

Iengine.prototype.write = function(splitId, pairs, more) {
    this.local.write(splitId, pairs, more);
    if (!more || typeof(more) == typeof(undefined)) {
        this.job.onMapComplete(splitId);
    }
};

Iengine.prototype.feed = function(splitId, bucketId, interUrls, target) {
    if (this.local.canhaz(splitId, bucketId)) {
        this.local.feed(splitId, bucketId, target);
    } else {
    	if(interUrls.length > 0) {
    		this.inter.feed(splitId, bucketId, interUrls, target);
    	} else {
    		console.log('master expects us to reduce stuff we can not reach');
    		console.log(this.local.items() + " local chunks available");
    		this.job.onChunkFail(splitId, bucketId);
    	}
    }
};

