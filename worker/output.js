function Output(job) {
    this.job = job;
    this.local = new Localstore()
}

Output.prototype.write = function(partitionId, pairs, more) {
    console.log("OUTPUT: " + JSON.stringify(pairs));
    if (! more) {
        this.job.onPartitionComplete(partitionId);
    }
}

