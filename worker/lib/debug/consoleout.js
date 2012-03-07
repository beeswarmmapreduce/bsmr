function consoleOut() {
    
    function Output(job) {
        this.job = job;
        this.pairs = [];
    }
    
    Output.prototype.write = function(bucketId, pairs, more) {
        this.pairs = this.pairs.concat(pairs);
        if (! more) {
            console.log("RESULT" + bucketId+ ": " + JSON.stringify(this.pairs));
            this.pairs = [];
            this.job.onBucketComplete(bucketId);
        }
    }
    
    var factory = function(job) {
    	return new Output(job);    	
    }
    
    return factory;
}   
