function Rcore(reducer) {
    this.reducer = reducer;
    this.results = [];
    this.step(undefined); //start it
}

Rcore.prototype.step = function(v2) {
    var ret = this.reducer.send(v2); //TODO: Exceptions?

    while(ret != undefined) {
        if(typeof(ret) != typeof("")) {
            throw ("task returned " + typeof(ret) + ", should be " + typeof(""));
        }
        this.results.push(ret)
        ret = this.reducer.next(); //TODO: Exceptions?
    }
}

Rcore.prototype.stop = function() {
    try {
        this.step(undefined);
    }
    catch(ex) {
        if (!(ex instanceof StopIteration)) {
            throw ex;
        }
        return this.results;
    }
    throw "Unreachable line";
}

