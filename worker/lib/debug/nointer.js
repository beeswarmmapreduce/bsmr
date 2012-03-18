function noInter() {
    
    function Inter(job) {
        this.job = job;
        var peerId = ('' + Math.random()).split('.')[1];
        job.setOwnPeerId('nointer://' + peerId);
    }
    
    Inter.prototype.feed = function(splitId, bucketId, urls, target) {
        var job = this.job;            
        for (var i in urls) {
            var url = urls[i];
        	job.markUnreachable(url);
        }
        job.onChunkFail(splitId, bucketId);
    };
    
    var factory = function(job, localStore) {
    	return new Inter(job);
    };
    
    return factory;
}
