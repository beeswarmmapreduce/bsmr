
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

Intermediary.prototype.startWrite = function(splitId) {
    this.splitId = splitId;
}

Intermediary.prototype.write = function(pairs, more) {
    for (i in pairs) {
        var pair = pairs[i];
        var key = pair[0];
        var partitionId = this._chooseBucket(key);
        this.local.put(this.splitId, partitionId, pair);
    }
    if (! more) {
        this.job.onMapComplete(this.splitId);
    }
}

Intermediary.prototype.start = function(partitionId, splitId) {
    this.rengine.startWrite(splitId);
    var chunk = this.local.get(partitionId, splitId);
    if (!chunk) {
        this.rengine.write([]);
    }
    this.rengine.write(chunk);
}


