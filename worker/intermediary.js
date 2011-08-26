
function Intermediary(R, rengine, job) {
    this.R = R;
    this.rengine = rengine;
    this.job = job;
    this.local = new Localstore();
}

Intermediary.prototype._chooseBucket = function(key) {
    var bucketId = 0;
    for (var i = 0; i < key.length; i++) {
        bucketId += key.charCodeAt(i);
        bucketId %= this.R;
    }
    return bucketId;
}

Intermediary.prototype.write = function(splitId, pairs, more) {
    for (i in pairs) {
        var pair = pairs[i];
        var key = pair[0];
        var partitionId = this._chooseBucket(key);
        this.local.write(splitId, partitionId, [pair]);
    }
    if (! more) {
        this.job.onMapComplete(splitId);
    }
}

Intermediary.prototype.start = function(splitId, partitionId, peerUrls) {
    if (this.local.canhaz(splitId)) {
        this.local.start(splitId, partitionId, this.rengine);
    } else {
        this.job.onUnreachable(splitId, partitionId, peerUrls);
    }
}


