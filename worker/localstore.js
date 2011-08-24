function Localstore() {
    this.local = {};
}

Localstore.prototype._storageId = function(partitionId, splitId) {
    var storageId = partitionId + '-' + splitId;
    if(!this.local[storageId]) {
        this.local[storageId] = [];
    }
    return storageId;
}

//get some data for a split
Localstore.prototype.get = function(partitionId, splitId) {
    var storageId = this._storageId(partitionId, splitId);
    var chunk = this.local[storageId].pop();
    return chunk;
}

//add some data for a split
Localstore.prototype.put = function(splitId, partitionId, content) {
    var storageId = this._storageId(partitionId, splitId);
    this.local[storageId].push(content);
}

//get a full partition
Localstore.prototype.getPartition = function(partitionId) {
    
}
