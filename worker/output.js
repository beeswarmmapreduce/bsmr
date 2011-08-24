function Output() {
    this.local = new Localstore()
}

Output.prototype.put = function(partitionId, splitId, x) {
    this.local.put(partitionId, splitId, x)
}

Output.prototype.write = function() {
    console.log(this.local.local);
}

