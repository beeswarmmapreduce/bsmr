function Inter(job) {
    this.job = job;
}

Inter.prototype.feed = function(splitId, partitionId, urls, target) {
    var failed = 0;

    var failure = function(url) {
        job.markUnreachable(url);
        failed += 1;
        if (failed >= urls.length) {
            this.job.onSplitFail(splitId, partitionId);
        }
    }

    var write = function(chunk, more) {
        target.write(splitId, partitionId, chunk, moer);
    }

    for (var i in urls) {
        var url = urls[i];
        request(url, write, failure);
    }
}

