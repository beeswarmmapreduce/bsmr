
params = {
    label: "SampoFS",
    baseUrl: "http://localhost:8080/fs/"
}

function outputPlugin(params, onData, onError) {
    this.baseUrl = params['baseUrl'];

    this.writeOutputFile = function(jobId, partitionId, data) {
        var url = this.baseUrl + 'partitions/' + jobId + '/' + partitionId + '/';

        // connect to fs and upload partition
        var req = new XMLHttpRequest();  
        req.open('POST', url, true);  
        req.onreadystatechange = function(aEvt) {  
            if (req.readyState == 4) {  
                if (req.status == 200) {
                    onData(partitionId, req.responseText);
                }
                else {
                    onError(req);
                }
            }  
        };
        req.overrideMimeType('text/plain');
        req.send(data);
    }
}

