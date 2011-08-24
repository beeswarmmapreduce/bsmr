function Output(job) {
    this.job = job;
    this.local = new Localstore()
}

Output.prototype.startWrite = function(partitionId) {
    this.partitionId = partitionId;
}

Output.prototype.write = function(pairs, more) {
    console.log(JSON.stringify(pairs));
    if (! more) {
        this.job.onPartitionComplete(this.paritionId);
    }
}

