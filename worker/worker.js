function Worker(masterUrl) {
    var HB_INTERVAL = 15 * 1000 //10s
    this.masterUrl = masterUrl;
    this._previousAction;
    this._job = {};
    this._callMaster();
    this._sendHeartbeats(HB_INTERVAL);
}

Worker.prototype._callMaster = function() {
    var PROTOCOL = "worker"
    this.ws = new WebSocket(this.masterUrl, PROTOCOL);
    var worker = this;
    this.ws.onopen = function() {
        worker._greet();
    }
    this.ws.onmessage = function(m) {
        var msg = JSON.parse(m.data);
        worker._react(msg);
    }
}

Worker.prototype._greet = function(msg) {
    var msg = {type: "ACK", payload: {action: "socket", protocol: "ws", port: 12345, resource: "omg"}};
    this.ws.send(JSON.stringify(msg));

}

Worker.prototype._react = function(msg) {
    var payload = msg.payload;
    var action = payload.action;
    this._previousAction = action;
    if (payload.job) {
        this._initjob(payload.job);
    }
    if (action == "mapTask") {
        var splitId = payload.mapStatus.splitId;
        this._job.onMap(splitId);
    }
    if (action == "reduceTask") {
        var partitionId = payload.reduceStatus.partitionId
        this._job.onReduceTask(partitionId);
    }
    if (action == "reduceSplit") {
        var partitionId = payload.reduceStatus.partitionId
        var splitId = payload.reduceStatus.splitId
        this._job.onReduceSplit(partitionId, splitId);
    }
}

Worker.prototype.reduceSplit = function(partitionId, splitId) {
        var msg = {type: "ACK", payload: {action: "reduceSplit", reduceStatus: {partitionId: partitionId, splitId: splitId}, unreachable: [], jobId: this._job.id}};
        this.ws.send(JSON.stringify(msg));
}

Worker.prototype.partitionComplete = function(partitionId) {
    var msg = {type: "ACK", payload: {action: "reduceTask", reduceStatus: {partitionId: partitionId}, unreachable: [], jobId: this._job.id}};
    this.ws.send(JSON.stringify(msg));
}

Worker.prototype._sendHeartbeats = function(interval) {
    var worker = this;
    var hb = function() {
        var msg = { "type": "HB", "payload": { "action": worker._previousAction, "jobId": worker.jobId}}
        worker.ws.send(JSON.stringify(msg));
    }
    setInterval(hb, interval);
}

Worker.prototype._initjob = function(requested) {
    if (this._job.id != requested.jobId) {
        eval(requested.code);
        requested.mapper = mapper;
        requested.reducer = reducer;
        if (typeof(combiner) != typeof(undefined)) {
            requested.combiner = combiner;
        }
        this._job = new Job(requested, this);
    }
}

Worker.prototype.mapComplete = function(splitId) {
    var msg = {type: "ACK", payload: {action: "mapTask", mapStatus: {splitId: splitId}, reduceStatus: null, jobId: this._job.id}};
    this.ws.send(JSON.stringify(msg));
}



