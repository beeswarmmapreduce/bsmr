function Rengine(reducer, output, job) {
  this.reducer = reducer;
  this.output = output;
  this.job = job;
  this.cores = {};
  this.saved = false;
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

Rengine.prototype.saveResults = function(bucketId) {
    for(var key in this.cores)
    {
        var values = this.cores[key].stop()
        for(var i in values) {
            var value = values[i];
            this.output.write(bucketId, [[key, value]], true);
        }
    }
    this.output.write(bucketId, [], false);
    this.saved = true;
}

Rengine.prototype.reset = function() {
	this.saved = false;
    this.cores = {};
}

Rengine.prototype.write = function(splitId, bucketId, pairs, more) {
	if (this.saved) {
		console.log('ERROR! Adding more data after saving results!');		
	}
	try {
    this._reduceSome(pairs);
	} catch (e) {
    	console.log('RED[' + bucketId + ',' + splitId + ',' + JSON.stringify(pairs) + ']');
    	throw 'AAARGH!';
	}

    if (!more) {
        this.job.onChunkComplete(splitId, bucketId);
    }
}

