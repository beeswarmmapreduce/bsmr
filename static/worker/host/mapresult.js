function Mapresult(chooseBucket, R) {
	this.chooseBucket = chooseBucket;
	this.R = R;
    this.chunks = [];
}

Mapresult.prototype._getchunk = function(bucketId) {
	var chunk = this.chunks[bucketId];
	if (typeof(chunk) == typeof(undefined)) {
		chunk = new Chunk();
	    this.chunks[bucketId] = chunk; 
	}
	return chunk;
};

Mapresult.prototype.write = function(splitId, pairs, more) {
    for (var i in pairs) {
        var pair = pairs[i];
        var key = pair[0];
        var bucketId = this.chooseBucket(key, this.R);
        var chunk = this._getchunk(bucketId);
        chunk.write(pair);
    }
};

Mapresult.prototype.feed = function(splitId, bucketId, target) {
	var chunk = this._getchunk(bucketId);
	chunk.feed(splitId, bucketId, target);
};