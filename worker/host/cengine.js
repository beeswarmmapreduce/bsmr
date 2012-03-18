function Cengine(combiner, output) {
  this.output = output;
  this.rtask = new Rtask(combiner);
}

Cengine.prototype.write = function(splitId, pairs, more) {
    this.rtask.reduceSome(pairs);
    if (!more) {
        this.rtask.feed(splitId, this.output);
    }
};

