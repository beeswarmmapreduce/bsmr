function Rengine(reducer, output) {
  this.reducer = reducer;
  this.output = output;
  this.gens = {}
}

Rengine.prototype.getgen = function(k2) {
    if (gens[k2] == undefined) {
        gens[k2] = new Rcore(this.reducer());
    }
    return gens[k2];
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

Rengine.prototype.getResults = function() {
    var results = {}
    for(var k2 in gens)
    {
        results[k2] = gens[k2].stop();
        gens[k2] = null;
        //delete gens[k2];
    }
    return results
}

Rengine.prototype.reduce = function(partitionId, splitId) {
}

