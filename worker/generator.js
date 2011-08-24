function StopIteration() { }

function Generator() {
    this.first = true;
    this.end = false;
    this.buffer = [];
}

Generator.prototype._step = function(y) { }

Generator.prototype._lastrun = function() {
    this.end = true;
}
Generator.prototype._emit = function(x) {
    this.buffer.unshift(x);
}
Generator.prototype.send = function(y) {
    if (this.end) {
        throw new StopIteration
    }
    if (this.first) {
	if(y != undefined) {
            throw new TypeError;
        }
        this.first = false;
        return undefined;
    }
    this._step(y)
    return this.buffer.pop();
}
Generator.prototype.next = function() {
    return this.send(undefined);
}
Generator.prototype.throw = function(e) {

}
Generator.prototype.close = function() {

}

