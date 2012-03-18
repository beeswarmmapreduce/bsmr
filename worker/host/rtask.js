function Rtask(reducer) {
	this.reducer = reducer;
	this.cores = {};
	this.dead = false;	  	  
}

Rtask.prototype._getcore = function(key) {
    if (this.cores[key] == undefined) {
        this.cores[key] = new Rcore(this.reducer());
    }
    return this.cores[key];
};

Rtask.prototype.reduceSome = function(pairs) {
	if(this.dead) {
		throw 'reduceSome called on a dead rtask';		
	}
    for(var i in pairs) {
        var pair = pairs[i];
        var key = pair[0];
        var value = pair[1];
        var core = this._getcore(key);
        core.reduce(value);
    }
};

Rtask.prototype.feed = function(id, target) {
	if(this.dead) {
		throw 'feed called on a dead rtask';		
	}
    for(var key in this.cores)
    {
        var values = this.cores[key].stop();
        for(var i in values) {
            var value = values[i];
            target.write(id, [[key, value]], true);
        }
    }
    target.write(id, [], false);
    this.dead = true;
};
