function ChunkRegistrar(chunks) {
	this.chunks = chunks;
	this.done = [];
}

ChunkRegistrar.prototype.allDone = function() {
    for (var i = 0; i < this.chunks; i++) {
        if (!this.done[i]) {
            return false;
        }
    }
    return true;
};

ChunkRegistrar.prototype._randInt = function(min, max) {
	return min + Math.floor(Math.random() * (max - min));
};

ChunkRegistrar.prototype.nextChunkID = function() {
	var incomplete = []; 
    for (var i = 0; i < this.chunks; i++) {
        if (!this.done[i]) {
        	incomplete.push(i);
        }
    }
    var left = incomplete.length;
    if (left > 0) {
    	var i = this._randInt(0, left);
    	return incomplete[i];
    }
};

ChunkRegistrar.prototype.markDone = function(splitId) {
    this.done[splitId] = true;
};
