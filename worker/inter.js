function Inter(R, job) {
    this.R = R;
    this.job = job;
}

Inter.prototype.chooseBucket = function(key) {
    var bucketId = 0;
    for (var i = 0; i < key.length; i++) {
        bucketId += key.charCodeAt(i);
        bucketId %= this.R;
    }
    return bucketId;
}

Inter.prototype.feed = function(splitId, partitionId, peerUrls, target) {
        this.job.onUnreachable(splitId, partitionId, peerUrls);
}

