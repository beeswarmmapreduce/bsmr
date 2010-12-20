
params = {
    label: "BrowserSocket"
}

function p2pPlugin(params, onData, onError) {
    this.fetchIntermediateData = function(jobId, partitionId, splitId, location) {
        var msg = null;

        // go over network to get the data
        worker.log('p2p:open: ' + partitionId + ', ' + splitId + ', ' + location, 'log', LOG_ERROR);
        var ws = new WebSocket(location);
        ws.onopen = function() {
            var m = worker.createMessage(worker.TYPE_P2P, {
                action: 'download',
                jobId: jobId,
                partitionId: partitionId,
                splitId: splitId,
                location: location
            });
            this.send(JSON.stringify(m));
        }
        ws.onmessage = function(e) {
            msg = worker.readMessage(e.data);
            if (msg) {
                if (msg.payload.action == 'upload') {
                    onData(msg.payload.jobId, msg.payload.partitionId, msg.payload.splitId, msg.payload.data);
                }
            }
            ws.close();
        }
        ws.onclose = function() {
            worker.log('p2p:close: ' + partitionId + ', ' + splitId + ', ' + location, 'log', LOG_ERROR);
            if (!msg) {
                onError(msg.payload.jobId, msg.payload.partitionId, msg.payload.splitId, location);
            }
        }
        ws.onerror = function() {
            worker.log('p2p:error: ' + partitionId + ', ' + splitId + ', ' + location, 'log', LOG_ERROR);
            onError(msg.payload.jobId, msg.payload.partitionId, msg.payload.splitId, location);
        }
    }
}


