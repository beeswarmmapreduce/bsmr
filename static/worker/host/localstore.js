function Localstore(chooseBucket, R) {
    this.chooseBucket = chooseBucket;
    this.R = R;
    this.results = new Mapresults();
    this.temporary = new Mapresults();
}

Localstore.prototype._commit = function(splitId) {
	if (!this.temporary.canhaz(splitId)) {
		console.error('commit failed, missing split-' + splitId);
	}
	if (this.results.canhaz(splitId)) {
		console.error('redundant work done for split-' + splitId);
	}
	var result = this.temporary.get(splitId);
    this.results.set(splitId, result);
    this.temporary.set(splitId, undefined);
};

Localstore.prototype.write = function(splitId, pairs, more) {
	if (!this.temporary.canhaz(splitId)) {
		var m = new Mapresult(this.chooseBucket, this.R);
        this.temporary.set(splitId, m);
	}
	var buffer = this.temporary.get(splitId);
    buffer.write(splitId, pairs, more);
    if (!more || typeof(more) == typeof(undefined)) {
        this._commit(splitId);
    }
};

Localstore.prototype.canhaz = function(splitId, bucketId) {
	return this.results.canhaz(splitId);
};

Localstore.prototype.splitcount = function() {
	return this.results.splitcount();
};

Localstore.prototype.feed = function(splitId, bucketId, target) {
	var mapresult = this.results.get(splitId);
    mapresult.feed(splitId, bucketId, target);
};
