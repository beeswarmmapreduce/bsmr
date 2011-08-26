function Rengine(reducer, output, job) {
  this.reducer = reducer;
  this.output = output;
  this.job = job;
  this.cores = {}
}

Rengine.prototype._getcore = function(key) {
    if (this.cores[key] == undefined) {
        this.cores[key] = new Rcore(this.reducer());
    }
    return this.cores[key];
}

Rengine.prototype._reduceSome = function(pairs) {
    for(var i in pairs) {
        var pair = pairs[i];
        var key = pair[0];
        var value = pair[1];
        var core = this._getcore(key);
        core.reduce(value);
    }
}

Rengine.prototype.saveResults = function(partitionId) {
    for(var key in this.cores)
    {
        var values = this.cores[key].stop()
        for(var i in values) {
            var value = values[i];
            this.output.write(partitionId, [[key, value]], true);
        }
    }
    this.output.write(partitionId, [], false);
}

Rengine.prototype.reset = function() {
    this.cores = {};
}

Rengine.prototype.write = function(splitId, pairs, more) {
    this._reduceSome(pairs);
    if (!more) {
        this.job.onSplitComplete(splitId);
    }
}

