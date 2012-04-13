function Mapresults() {
    this.mapresults = {};
}

Mapresults.prototype.set = function(splitId, m) {
	this.mapresults[splitId] = m;
};

Mapresults.prototype.get = function(splitId) {
	var mapresult = this.mapresults[splitId];
	if (typeof(mapresult) == typeof(undefined)) {
		return null;
	}
	return mapresult;
};

Mapresults.prototype.canhaz = function(splitId) {
	var mapresult = this.get(splitId);
	if (mapresult == null) {
		return false;
	}
	return true;
};

Mapresults.prototype.splitcount = function() {
	var count = 0;
	for(undefined in this.mapresults) {
		count += 1;			
	}
	return count;
};
