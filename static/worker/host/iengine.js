
function Iengine(R, job, inter, chooseBucket) {
    this.R = R;
    this.job = job;
    this.local = new Localstore(chooseBucket);
    this.inter = inter(job, this.local);
    this.chooseBucket = chooseBucket;
}

Iengine.prototype.write = function(splitId, pairs, more) {
    for (i in pairs) {
        var pair = pairs[i];
        var key = pair[0];
        var bucketId = this.chooseBucket(key, this.R);
        this.local.write(splitId, bucketId, [pair], true);
    }
    if (! more) {
        for (var bucketId = 0; bucketId < this.R; bucketId += 1) {
            this.local.write(splitId, bucketId, [], false);
        }
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

