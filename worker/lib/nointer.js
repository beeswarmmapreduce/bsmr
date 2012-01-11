function noInter(job, localStore) {
    
    function Inter(job) {
        this.job = job;
    }
    
    Inter.prototype.feed = function(splitId, partitionId, urls, target) {
        var failed = 0;
        var job = this.job;
    
        var failure = function(url) {
        	job.markUnreachable(url);
            failed += 1;
            if (failed >= urls.length) {
                job.onSplitFail(splitId, partitionId);
            }
        }
    
        var write = function(chunk, more) {
            target.write(splitId, partitionId, chunk, more);
        }
    
        var request = function(url, write, failure) {
            failure(url); //THIS IS A DUMMY THAT FAILS ALWAYS
        }
    
        for (var i in urls) {
            var url = urls[i];
            request(url, write, failure);
        }
    }
    return new Inter(job);
}
