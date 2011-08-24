function Rengine(reducer, output, job) {
  this.reducer = reducer;
  this.output = output;
  this.job = job;
  this.cores = {}
}

Rengine.prototype.getcore = function(key) {
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
        var core = this.getcore(key);
        core.reduce(value);
    }
}

Rengine.prototype.saveResults = function() {
    for(var key in this.cores)
    {
        var values = this.cores[key].stop()
        for(var value in values) {
            this.output.write([[key, value]], true);
        }
    }
    this.output.write([], false);
}

Rengine.prototype.reset = function(partitionId, splitId) {
    this.cores = {};
}

Rengine.prototype.startWrite = function(splitId) {
    this.split = splitId;
}

Rengine.prototype.write = function(pairs, more) {
    this._reduceSome(pairs);
    if (!more) {
        this.job.onSplitComplete(this.splitId);
    }
}

