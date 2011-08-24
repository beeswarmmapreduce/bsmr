
function Intermediary(R, next, job) {
    this.R = R;
    this.local = new Localstore();
    this.job = job;
}

Intermediary.prototype._chooseBucket = function(key) {
    var bucketId = 0;
    for (var i = 0; i < key.length; i++) {
        bucketId += key.charCodeAt(i);
        bucketId %= this.R;
    }
    return bucketId;
}

Intermediary.prototype.get = function(partitionId, splitId, cb) {
    var chunk = this.local.get(partitionId, splitId);
    cb(chunk);
}

Intermediary.prototype.write = function(pairs) {
    for (i in pairs) {
        var pair = pairs[i];
        var partitionId = this._chooseBucket(pair[0]);
        this.local.put(this.splitId, partitionId, pair);
    }
}

Intermediary.prototype.startWrite = function(splitId) {
    this.splitId = splitId;
}

Intermediary.prototype.endWrite = function() {
    console.log(JSON.stringify(this.local.local));
    this.job.onMapComplete(this.splitId);
}

