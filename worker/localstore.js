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
}

Localstore.prototype._commit = function(splitId, bucketId) {
    if(!this.local[splitId]) {
        this.local[splitId] = {};
    }
    this.local[splitId][bucketId] = this.tmp[splitId][bucketId];
    this.tmp[splitId][bucketId] = undefined;
}

Localstore.prototype.write = function(splitId, bucketId, content, more) {
    this._write(splitId, bucketId, content);
    if (!more) {
        this._commit(splitId, bucketId);
    }
}

Localstore.prototype.canhaz = function(splitId, bucketId) {
	var split = this.local[splitId];
	if (typeof(split) == typeof(undefined)) {
		console.log("Localstore::canhaz(), split was undefined");
		return false;
	}
	var chunks = split[bucketId];
	if (typeof(chunks) == typeof(undefined)) {
		console.log("Localstore::canhaz(), chunks was undefined");
		return false;
	}
	return true;
}

Localstore.prototype.feed = function(splitId, bucketId, target) {
    var split = this.local[splitId];
    var chunks = split[bucketId];
    for (var i in chunks) {
        var chunk = chunks[i];
        target.write(splitId, bucketId, chunk, true);
    }
    target.write(splitId, bucketId, [], false);
}
