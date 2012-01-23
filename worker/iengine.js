
function Iengine(R, job, inter, chooseBucket) {
    this.R = R;
    this.job = job;
    this.local = new Localstore();
    this.inter = inter(job, this.local);
    this.chooseBucket = chooseBucket;
}

Iengine.prototype.write = function(splitId, pairs, more) {
    for (i in pairs) {
        var pair = pairs[i];
        var key = pair[0];
        var bucketId = this.chooseBucket(key);
        this.local.write(splitId, bucketId, [pair], true);
    }
    if (! more) {
        for (var bucketId = 0; bucketId < this.R; bucketId += 1) {
            this.local.write(splitId, bucketId, [], false);
        }
        this.job.onMapComplete(splitId);
    }
}

Iengine.prototype.feed = function(splitId, bucketId, peerUrls, target) {
    if (this.local.canhaz(splitId, bucketId)) {
        this.local.feed(splitId, bucketId, target);
    } else {
        this.inter.feed(splitId, bucketId, peerUrls, target);
    }
}

