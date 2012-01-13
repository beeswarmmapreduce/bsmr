function debugInput(M) {
    
    function Input(M) {
        this.M = M;
    }
    
    Input.prototype.feed = function(splitId, target) {
        var fake = new Fake(splitId, this.M);
        fake.ondata = function(some) {
            var gotMore = fake.items() > 0;
            target.write(splitId, some, gotMore);
        }
        var items = fake.items();
        for (var i = 0; i < items; i++) {
            fake.request()
        }
    }
    
    function Fake(splitId, M) {
        this.ret = this._repe(15);
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
        var current = [];
        for (var i in pairs) {
            pair = pairs[i];
            current.push(pair);
            if (Math.random() > 0.5) {
                buffer.push(current);
                current = [];
            }
        }
        buffer.push(current);
        return buffer;
    }
    
    Fake.prototype._createseq = function(splitId, M) {
        var rlen = this.ret.length;
        var splitLen = Math.floor(rlen / M) + 1;
        var s = this.ret.substr(splitId * splitLen, splitLen)
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

    return new Input(M);
}
   
