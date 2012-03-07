function petriFSOut() {
    
    function Output(job) {
        this.job = job;
        this.pairs = [];
    }
    
    Output.prototype.write = function(bucketId, pairs, more) {
    	var outputplug = this;
        this.pairs = this.pairs.concat(pairs);
        if (! more) {
            var result = JSON.stringify(this.pairs);
            var xhr = new XMLHttpRequest();  
            xhr.open("POST", "http://localhost:8080/fs/filesystem?operation=write&filename=" + outputplug.job.id + '-' + bucketId + ".json&begin=0&length=" + result.length, true);  
            xhr.onload = function(oEvent) {  
              if (xhr.status == 200) {  
                outputplug.job.onBucketComplete(bucketId);
              } else {
            	  throw 'Failed to store results!';
              }  
            };
            xhr.send(result);  
            this.pairs = [];
        }
    }
    
    var factory = function(job) {
    	return new Output(job);    	
    }
    
    return factory;
}   
