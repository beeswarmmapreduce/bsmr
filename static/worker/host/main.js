function parseParams() {
    var query = window.location.href.split('?')[1];
    var defs = [];
    if (query !== undefined) {
        defs = query.split('&');
    }
    var params = {};
    for (var i in defs) {
        var parts = defs[i].split("=");
        params[parts[0]] = parts[1];
    }
    return params;
}

var params = parseParams();

var masterurl = 'ws://' + params.master + '/bsmr';

new Worker(masterurl);

