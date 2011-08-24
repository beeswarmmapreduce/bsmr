
function Mengine(mapper, inter) {
    this.mapper = mapper;
    this.inter = inter;
}

Mengine.prototype.write = function(pairs, gotMore) {
    var inter = this.inter;
    var writer = function(pairs) {
        inter.write(pairs);
    }
    for(var i in pairs) {
        this.mapper(pairs[i], writer);
    }
    if (!gotMore) {
        this.inter.endWrite();
    }
}

