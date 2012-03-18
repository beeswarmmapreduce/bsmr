/*
This is a convenience library for defining javascript generators without yield. While yield provides perhaps superrior syntax it is not available on all browsers. Below you can find two code examples of a reducer that adds numbers. The first one is written using yield, while the second one uses this library. For the user the two functions should seem equal.

  YIELDFUL VERSION:

reducer = function() {
    var x = 0;
    while (y = yield) {
        x += parseInt(y, 10);
    }
    yield x.toString();
}

  YIELDLESS VERSION:

reducer = function() {
    function Adder() {
        this.x = 0;
    }
    Adder.prototype = new Generator();
    Adder.prototype._step = function(y) {
        if (y) {
            this.x += parseInt(y, 10);
        }
        else {
            this._lastrun();
            var result = this.x.toString();
            this._emit(result);
        }
    }
    return new Adder();
}


*/

function StopIteration() { }

function Generator() {
    this.first = true;
    this.end = false;
    this.buffer = [];
}

Generator.prototype._step = function(y) { };

Generator.prototype._lastrun = function() {
    this.end = true;
};
Generator.prototype._emit = function(x) {
    this.buffer.unshift(x);
};
Generator.prototype.send = function(y) {
    if (this.end) {
        throw new StopIteration;
    }
    if (this.first) {
	if(y != undefined) {
            throw new TypeError;
        }
        this.first = false;
        return undefined;
    }
    this._step(y);
    return this.buffer.pop();
};
Generator.prototype.next = function() {
    return this.send(undefined);
};

/* The Standard Generator API defines these two methods, but we do not use them.
Generator.prototype.throw = function(e) {

};
Generator.prototype.close = function() {

};
*/
