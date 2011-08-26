function Cengine(combiner, output) {
  this.combiner = combiner;
  this.output = output;
  this.cores = {}
}

Cengine.prototype._getcore = function(key) {
    if (this.cores[key] == undefined) {
        this.cores[key] = new Rcore(this.combiner());
    }
    return this.cores[key];
}

Cengine.prototype._reduceSome = function(pairs) {
    for(var i in pairs) {
        var pair = pairs[i];
        var key = pair[0];
        var value = pair[1];
        var core = this._getcore(key);
        core.reduce(value);
    }
}

Cengine.prototype._complete = function(splitId) {
    for(var key in this.cores)
    {
        var values = this.cores[key].stop()
        for(var i in values) {
            var value = values[i];
            this.output.write(splitId, [[key, value]], true);
        }
    }
    this.output.write(splitId, [], false);
    this._reset();
}

Cengine.prototype._reset = function(partitionId, splitId) {
    this.cores = {};
}

Cengine.prototype.write = function(splitId, pairs, more) {
    this._reduceSome(pairs);
    if (!more) {
        this._complete(splitId);
    }
}

