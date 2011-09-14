function consoleOut(job) {
    
    function Output(job) {
        this.job = job;
        this.pairs = [];
    }
    
    Output.prototype.write = function(partitionId, pairs, more) {
        this.pairs = this.pairs.concat(pairs);
        if (! more) {
            console.log("PARTITION" + partitionId+ ": " + JSON.stringify(this.pairs));
            this.pairs = [];
            this.job.onPartitionComplete(partitionId);
        }
    }
    return new Output(job);
}   
