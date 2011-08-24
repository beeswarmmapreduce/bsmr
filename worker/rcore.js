function Rcore(reducer) {
    this.reducer = reducer;
    this.results = [];
    this._step(undefined); //start it
}

Rcore.prototype.reduce = function(value) {
    if (value) {
        this._step(value);
    }
}

Rcore.prototype._step = function(v2) {
    var ret = this.reducer.send(v2); //TODO: Exceptions?

    while(typeof(ret) != typeof(undefined)) {
        if(typeof(ret) != typeof("")) {
            throw ("task returned " + typeof(ret) + ", should be " + typeof(""));
        }
        this.results.push(ret)
        ret = this.reducer.next(); //TODO: Exceptions?
    }
}

Rcore.prototype.stop = function() {
    try {
        this._step(undefined);
    }
    catch(ex) {
        if (!(ex instanceof StopIteration)) {
            throw ex;
        }
        return this.results;
    }
    throw "Unreachable line";
}

