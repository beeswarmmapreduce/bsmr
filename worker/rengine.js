function Rengine(reducer, output, job) {
  this.reducer = reducer;
  this.output = output;
  this.job = job;
  this.reducers = {}
}

Rengine.prototype.getgen = function(k2) {
    if (reducers[k2] == undefined) {
        reducers[k2] = new Rcore(this.reducer());
    }
    return reducers[k2];
}

Rengine.prototype.reduceData = function(chunk) {
    for(var k2 in chunk) {
        gen = getgen(k2);
        var values = chunk[k2];
        for(var v in values) {
            gen.step(values[v]);
        }
    }
}

Rengine.prototype.saveResults = function() {
    for(var key in this.reducers)
    {
        var values = this.reducers[key].stop()
        for(var value in values) {
            this.output.write([[key, value]], true);
        }
    }
    this.output.write([], false);
}

Rengine.prototype.reset = function(partitionId, splitId) {
  this.gens = {}
}

Rengine.prototype.reduce = function(partitionId, splitId) {
    this.job.onSplitComplete(splitId);
}

