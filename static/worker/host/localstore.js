function Localstore(chooseBucket) {
    this.chooseBucket = chooseBucket;
    this.local = {};
    this.tmp = {};
}

Localstore.prototype._write = function(splitId, bucketId, content) {
    if(!this.tmp[splitId]) {
        this.tmp[splitId] = new Chunkstore();
    }
    this.tmp[splitId].write(bucketId, content, true);
};

Localstore.prototype._commit = function(splitId, bucketId) {
    this.local[splitId] = this.tmp[splitId];
    this.tmp[splitId] = undefined;
};

Localstore.prototype._getChunk = function(splitId, bucketId) {
	var chunks = this.local[splitId];
	if (typeof(chunks) == typeof(undefined)) {
		return null;
	}
	return chunks._getChunk(bucketId);
};

Localstore.prototype.write = function(splitId, bucketId, content, more) {
    this._write(splitId, bucketId, content);
    if (!more) {
        this._commit(splitId, bucketId);
    }
};

Localstore.prototype.canhaz = function(splitId, bucketId) {
	var chunks = this.local[splitId];
	return typeof(chunks) != typeof(undefined);
};

Localstore.prototype.splitcount = function() {
	var count = 0;
	for(undefined in this.local) {
		count += 1;			
	}
	return count;
};

Localstore.prototype.feed = function(splitId, bucketId, target) {
    var chunks = this.local[splitId];
    chunks.feed(splitId, bucketId, target);
};
