function Localstore(chooseBucket, R) {
    this.chooseBucket = chooseBucket;
    this.R = R;
    this.mapresults = {};
    this.buffers = {};
}

Localstore.prototype._commit = function(splitId) {
    this.mapresults[splitId] = this.tmpresults[splitId];
    this.tmpresults[splitId] = undefined;
};

Localstore.prototype._getmapresult = function(splitId) {
	var mapresult = this.mapresults[splitId];
	if (typeof(mapresult) == typeof(undefined)) {
		return null;
	}
	return mapresult;
};

Localstore.prototype._getbuffer = function(splitId) {
	var buffer = this.buffers[splitId];
	if (typeof(buffer) == typeof(undefined)) {
		buffer = new Mapresult();
	    this.buffers[splitId] = buffer;
	}
	return buffer;
};

Iengine.prototype.write = function(splitId, pairs, more) {
	var buffer = this._getbuffer(splitId);
    buffer.write(splitId, pairs, more);
    if (!more) {
        this._commit(splitId);
    }
};

Localstore.prototype.canhaz = function(splitId, bucketId) {
	var mapresult = this._getmapresult(splitId);
	if (mapresult == null) {
		return false;
	}
	return true;
};

Localstore.prototype.splitcount = function() {
	var count = 0;
	for(undefined in this.mapresults) {
		count += 1;			
	}
	return count;
};

Localstore.prototype.feed = function(splitId, bucketId, target) {
	var mapresult = this._getmapresult(splitId);
    mapresult.feed(splitId, bucketId, target);
};
