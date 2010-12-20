
params = {
    label: "SampoFS",
    baseUrl: "http://localhost:8080/fs/"
}

function inputPlugin(params, onData, onError) {
    this.baseUrl = params['baseUrl'];

    this.fetchMapData = function(jobId, splitId) {
        var url = this.baseUrl + 'splits/' + jobId + '/' + splitId + '/';

        // connect to fs and retrieve split
        var req = new XMLHttpRequest();  
        req.open('GET', url, true);  
        req.onreadystatechange = function(aEvt) {  
            if (req.readyState == 4) {  
                if (req.status == 200) {
                    onData(jobId, splitId, req.responseText);
                }
                else {
                    onError(req);
                }
            }  
        };
        req.send(null);  
    }
}

