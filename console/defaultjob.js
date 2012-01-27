mapper = function(pair, emit) {
    var word = pair[1];
    emit(word, '1');
}

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

combiner = reducer;

chooseBucket = boringBucket;

input = wordInput('loremipsum.txt');
inter = flashInter();
output = petriFSOut();
