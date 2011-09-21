function flashInter(job, local) {

    function Inter(job, local) {
        this.job = job;
        this.local = local
        this.buffer = [];
    }

    //server side

    Inter.prototype.write = function(splitId, partitionId, pairs, more) {
        this.buffer = this.buffer.concat(pairs)
        if (!more) {
            send(splitId, partitionId, this.buffer)
            this.buffer = [];
        }
    }

    Inter.prototype.fail = function(splitId, partitionId) {
        //BUG: there is not fail
        fail(splitId, partitionId)
    }
    
    //BUG: no one calls onRequest
    Inter.prototype.onRequest = function(splitId, partitionId) {
        this.remoteFeed(splitId, partitionId, this);
    }

    Inter.prototype.remoteFeed = function(splitId, partitionId, remoteTarget) {
        if (this.local.canhaz(splitId)) {
            this.local.feed(splitId, partitionId, remoteTarget);
        }
        remoteTarget.fail(splitId, partitionId);
    }

    //client side

    Inter.prototype.feed = function(splitId, partitionId, urls, target) {
        var failed = 0;
        var job = this;
    
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
            failure(url); //BUG: THIS IS A DUMMY THAT FAILS ALWAYS
        }
    
        for (var i in urls) {
            var url = urls[i];
            request(url, write, failure);
        }
    }

    return new Inter(job, local);
}
