function Localstore() {
    this.local = {};
    this.tmp = {};
}

Localstore.prototype._write = function(splitId, bucketId, content) {
    if(!this.tmp[splitId]) {
        this.tmp[splitId] = {};
    }
    if(!this.tmp[splitId][bucketId]) {
        this.tmp[splitId][bucketId] = [];
    }
    this.tmp[splitId][bucketId].push(content);
};

Localstore.prototype._commit = function(splitId, bucketId) {
    if(!this.local[splitId]) {
        this.local[splitId] = {};
    }
    this.local[splitId][bucketId] = this.tmp[splitId][bucketId];
    this.tmp[splitId][bucketId] = undefined;
};

Localstore.prototype.write = function(splitId, bucketId, content, more) {
    this._write(splitId, bucketId, content);
    if (!more) {
        this._commit(splitId, bucketId);
    }
};

Localstore.prototype.canhaz = function(splitId, bucketId) {
	var chunks = this.local[splitId];
	if (typeof(chunks) == typeof(undefined)) {
		return false;
	}
	var chunk = chunks[bucketId];
	if (typeof(chunk) == typeof(undefined)) {
		return false;
	}
	return true;
};

Localstore.prototype.feed = function(splitId, bucketId, target) {
    var chunks = this.local[splitId];
    var chunk = chunks[bucketId];
    for (var i in chunk) {
        var frame = chunk[i];
        target.write(splitId, bucketId, frame, true);
    }
    target.write(splitId, bucketId, [], false);
};
