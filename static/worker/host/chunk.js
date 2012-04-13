function Chunk() {
    this.pairs = [];
}

Chunk.prototype.write = function(pair) {
    this.pairs.push(pair);
};

Chunk.prototype.feed = function(splitId, bucketId, target) {
    target.write(splitId, bucketId, this.pairs);	
};