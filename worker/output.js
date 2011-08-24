function Output(job) {
    this.job = job;
    this.local = new Localstore()
}

Output.prototype.put = function(partitionId, splitId, x) {
    this.local.put(partitionId, splitId, x)
}

Output.prototype.write = function(pairs, more) {
    console.log(this.local.local);
    if (! more) {
        this.job.onPartitionComplete();
    }
}

