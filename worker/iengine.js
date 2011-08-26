
function Iengine(R, job, chooseBucket) {
    this.R = R;
    this.job = job;
    this.local = new Localstore();
    this.inter = new Inter(job)
    this.chooseBucket = chooseBucket;
}

Iengine.prototype.write = function(splitId, pairs, more) {
    for (i in pairs) {
        var pair = pairs[i];
        var key = pair[0];
        var partitionId = this.chooseBucket(key);
        this.local.write(splitId, partitionId, [pair], true);
    }
    if (! more) {
        for (var partitionId = 0; partitionId < this.R; partitionId += 1) {
            this.local.write(splitId, partitionId, [], false);
        }
        this.job.onMapComplete(splitId);
    }
}

Iengine.prototype.feed = function(splitId, partitionId, peerUrls, target) {
    if (this.local.canhaz(splitId)) {
        this.local.feed(splitId, partitionId, target);
    } else {
        this.inter.feed(splitId, partitionId, peerUrls, target);
    }
}

