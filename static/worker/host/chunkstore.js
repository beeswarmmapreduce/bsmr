function Chunkstore() {
    this.local = {};
    this.tmp = {};
}

Chunkstore.prototype._write = function(bucketId, content) {
    if(!this.tmp[bucketId]) {
        this.tmp[bucketId] = [];
    }
    this.tmp[bucketId].push(content);
};

Chunkstore.prototype._commit = function(bucketId) {
    this.local[bucketId] = this.tmp[bucketId];
    this.tmp[bucketId] = undefined;
};

Chunkstore.prototype._getChunk = function(bucketId) {
	var chunk = this.local[bucketId];
	if (typeof(chunk) == typeof(undefined)) {
		return [];
	}
	return chunk;
};

Chunkstore.prototype.write = function(bucketId, content, more) {
    this._write(bucketId, content);
    if (!more) {
        this._commit(bucketId);
    }
};

Chunkstore.prototype.feed = function(splitId, bucketId, target) {
    var chunk = this._getChunk(bucketId);
    for (var i in chunk) {
        var frame = chunk[i];
        target.write(splitId, bucketId, frame, true);
    }
    target.write(splitId, bucketId, [], false);
};
