
function Mengine(mapper, inter) {
    this.mapper = mapper;
    this.inter = inter;
}

Mengine.prototype.write = function(pairs, gotMore) {
    var inter = this.inter;
    var emit = function(key, value) {
        var pair = [key, value];
        inter.write([pair], true);
    }
    for(var i in pairs) {
        this.mapper(pairs[i], emit);
    }
    if (!gotMore) {
        inter.write([], false);
    }
}

