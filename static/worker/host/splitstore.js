function Splitstore() {
    this.buckets = [];
}

Splitstore.prototype.write2 = function(splitId, bucketId, content, more) {
    this._write(splitId, bucketId, content);
    if (!more) {
        this._commit(splitId, bucketId);
    }
};

Splitstore.prototype.write = function(splitId, pairs, more) {
    for (i in pairs) {
        var pair = pairs[i];
        var key = pair[0];
        var bucketId = this.chooseBucket(key, this.R);
        this.write2(splitId, bucketId, [pair], true);
    }
    if (! more) {
        for (var bucketId = 0; bucketId < this.R; bucketId += 1) {
            this.write2(splitId, bucketId, [], false);
        }
    }
};