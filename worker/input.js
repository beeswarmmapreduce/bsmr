
function Input(M, target) {
    this.M = M;
    this.target = target;
}

Input.prototype.start = function(splitId) {
    var input = this;
    var fake = new Fake(splitId, this.M);
    fake.ondata = function(some) {
        var gotMore = fake.items() > 0;
        input.target.write(some, gotMore);
    }
    var items = fake.items();
    for (var i = 0; i < items; i++) {
        fake.request()
    }
}

function Fake(splitId, M) {
    var pairs = this._createseq(splitId, M);
    this._buffer = this._parts(pairs);
}

Fake.prototype.items = function() {
    return this._buffer.length;
}

Fake.prototype.request = function() {
    this.ondata(this._buffer.pop());
}

Fake.prototype._parts = function(pairs) {
    var buffer = [];
    var part = [];
    var i = 0;
    var pair;
    while (pair = pairs.pop()) {
        if (i > 4) {
            buffer.push(part);
            i = 0;
            parts = [];
        }
        part.push(pair);
        i += 1;
    }
    return buffer;
}

Fake.prototype._createseq = function(splitId, M) {
    var ret = this._repe(7);
    var rlen = ret.length;
    var splitLen = Math.floor(rlen / M);
    var s = ret.substr(splitId * splitLen, splitLen)
    var chars = s.split("")
    var pairs = Array.prototype.map.call(chars, this._makepair);
    return pairs;
}


Fake.prototype._makepair = function(x) {
    return ['filename', x];
}

Fake.prototype._alphabet = function(i) {
    var a = 97;
    return String.fromCharCode(a + i);
}

Fake.prototype._repe = function(repeat) {
        var ret = '';
        for (var i = 0; i < repeat; i++) {
                for (var j = 0; j < Math.pow(2, (i + 1)); j++) {
                        ret += this._alphabet(i);
                        }
                }
    return ret;
}

