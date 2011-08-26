function Localstore() {
    this.local = {};
    this.tmp = {};
}

Localstore.prototype._write = function(splitId, partitionId, content) {
    if(!this.tmp[splitId]) {
        this.tmp[splitId] = {};
    }
    if(!this.tmp[splitId][partitionId]) {
        this.tmp[splitId][partitionId] = [];
    }
    this.tmp[splitId][partitionId].push(content);
}

Localstore.prototype._commit = function(splitId, partitionId) {
    if(!this.local[splitId]) {
        this.local[splitId] = {};
    }
    this.local[splitId][partitionId] = this.tmp[splitId][partitionId];
    this.tmp[splitId][partitionId] = undefined;
}

Localstore.prototype.write = function(splitId, partitionId, content, more) {
    this._write(splitId, partitionId, content);
    if (!more) {
        this._commit(splitId, partitionId);
    }
}

Localstore.prototype.canhaz = function(splitId) {
    return typeof(this.local[splitId]) != typeof(undefined);
}

Localstore.prototype.feed = function(splitId, partitionId, target) {
    var split = this.local[splitId];
    var chunks = split[partitionId];
    for (var i in chunks) {
        var chunk = chunks[i];
        target.write(splitId, partitionId, chunk, true);
    }
    target.write(splitId, partitionId, [], false);
}

/*
Localstore.prototype.getPartition = function(partitionId) {
    
}

*/
