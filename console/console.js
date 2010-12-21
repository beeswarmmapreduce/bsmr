/**
 konk.js

 BrowserSocket MapReduce
 konk to the master
 
 Konrad Markus <konker@gmail.com>
 */

var konk = (function() {
    var CODE = 0;
    var PARAMS = 1;

    return {
        TYPE_ADDJOB: "ADDJOB",
        TYPE_REMOVEJOB: "REMOVEJOB",

        masterUrl: null,
        ws: null,

        init: function() {
            konk.plugins.init();
            $('#connect').bind('click', konk.connect);
            $('#addJob').bind('click', konk.jobs.queue.add);
            var l = document.location.toString();
            var ms = l.match(/:\/\/([^\/]*)\//);
            if (ms && ms[1]) {
                $('#master').val('ws://' + ms[1] + '/bsmr');
            }
        },
        connect: function() {
            if (konk.ws) {
                konk.disconnect();
            }
            konk.masterUrl = $('#master').val();
            konk.ws = new WebSocket(konk.masterUrl, "console");
            konk.ws.onopen = konk.onopen;
            konk.ws.onmessage = konk.onmessage;
            konk.ws.onerror = konk.onerror;
            konk.ws.onclose = konk.onclose;
        },
        disconnect: function(e) {
            konk.ws.close();
        },
        onopen: function(e) { 
            $('#master').attr('disabled', 'disabled');
            $('#connect').unbind('click').bind('click', konk.disconnect).val('disconnect');
        },
        onerror: function(e) {
            alert('error');
        },
        onclose: function(e) {
            $('#master').removeAttr('disabled');
            $('#connect').unbind('click').bind('click', konk.connect).val('connect');
        },
        onmessage: function(e) {
            var data = JSON.parse(e.data);
            konk.log(data.payload);
            konk.work.render(data.payload);
            konk.workers.render(data.payload.workers);
            konk.jobs.active.render(data.payload.job);
            konk.jobs.queue.render(data.payload.jobQueue);
            konk.jobs.history.render(data.payload.jobHistory);
        },
        plugins: {
            plugins: {
                in: {},
                p2p: {},
                out: {}
            },
            n: 0,

            init: function() {
                // fetch the plugins list
                var req = new XMLHttpRequest();  
                req.open('GET', '/console/plugins/plugins.lst', true);  
                req.onreadystatechange = function(aEvt) {  
                    if (req.readyState == 4) {  
                        if (req.status == 200) {
                            konk.plugins.processList(req.responseText);
                        }
                        else {
                            /*[FIXME: beter error handling?]*/
                            konk.log('plugins list failed: ' + req.status);
                        }
                    }  
                };
                req.overrideMimeType('text/plain');
                req.send(null);
            },
            processList: function(raw) {
                konk.log(raw);
                var ls = raw.split(/\n/);
                konk.plugins.n = 0;
                for (var l in ls) {
                    if (ls[l] != 'plugins.lst' && ls[l] != '') {
                        konk.plugins.n++;
                        konk.plugins.fetchPlugin(ls[l]);
                    }
                }
            },
            fetchPlugin: function(pluginId) {
                // fetch the plugins list
                var req = new XMLHttpRequest();  
                req.open('GET', '/console/plugins/' + pluginId, true);  
                req.onreadystatechange = function(aEvt) {  
                    if (req.readyState == 4) {  
                        if (req.status == 200) {
                            konk.plugins.processPlugin(pluginId, req.responseText);
                        }
                        else {
                            /*[FIXME: beter error handling?]*/
                            konk.log('plugins fetch failed: ' + req.status);
                        }
                    }  
                };
                req.overrideMimeType('text/plain');
                req.send(null);
            },
            processPlugin: function(pluginId, raw) {
                konk.log(konk.plugins.n);
                konk.log('processing ' + pluginId);
                try {
                    eval(raw);
                    var p = null;
                    if (pluginId.indexOf('in_') == 0) {
                        p = [ inputPlugin, params ];
                        konk.plugins.plugins.in[pluginId] = p;
                    }
                    else if (pluginId.indexOf('p2p_') == 0) {
                        p = [ p2pPlugin, params ];
                        konk.plugins.plugins.p2p[pluginId] = p;
                    }
                    if (pluginId.indexOf('out_') == 0) {
                        p = [ outputPlugin, params ];
                        konk.plugins.plugins.out[pluginId] = p;
                    }

                    if (konk.plugins.numLoaded() == konk.plugins.n) {
                        konk.plugins.render();
                    }
                }
                catch(ex) {
                    konk.log('plugin eval failed: ' + ex);
                }
            },
            render: function() {
                konk.log(konk.plugins.plugins);
                // input plugins
                for (var p in konk.plugins.plugins.in) {
                    $('#inputPlugin').append('<option value="' + p + '">' + konk.plugins.plugins.in[p][PARAMS].label +'</option>');
                }
                $('#inputPlugin').bind('change', konk.plugins.pluginChange);

                // p2p plugins
                for (var p in konk.plugins.plugins.p2p) {
                    $('#p2pPlugin').append('<option value="' + p + '">' + konk.plugins.plugins.p2p[p][PARAMS].label +'</option>');
                }
                $('#p2pPlugin').bind('change', konk.plugins.pluginChange);

                // out plugins
                for (var p in konk.plugins.plugins.out) {
                    $('#outputPlugin').append('<option value="' + p + '">' + konk.plugins.plugins.out[p][PARAMS].label +'</option>');
                }
                $('#outputPlugin').bind('change', konk.plugins.pluginChange);
            },
            renderParams: function(list, p) {
                var l = $(list);
                if (p == 'null') {
                    l.html('');
                    return;
                }
                var src = null;
                var target = null;
                var prefix = '';
                if (l.attr('id') == 'inputPlugin') {
                    src = konk.plugins.plugins.in;
                    target = $('#inputPluginParams');
                    prefix = 'in_';
                }
                else if (l.attr('id') == 'p2pPlugin') {
                    src = konk.plugins.plugins.p2p;
                    target = $('#p2pPluginParams');
                    prefix = 'p2p_';
                }
                else if (l.attr('id') == 'outputPlugin') {
                    src = konk.plugins.plugins.out;
                    target = $('#outputPluginParams');
                    prefix = 'out_';
                }
                if (src != null && target != null) {
                    target.empty();
                    for (var i in src[p][PARAMS]) {
                        if (i != 'label') {
                            var s = '<p>';
                            s += '<label for="' + prefix + i+ '">' + i + ':</label> ';
                            s += '<input type="text" id="' + prefix + i + '" value="' + src[p][PARAMS][i] + '"/>';
                            s += '</p>';
                            target.append(s);
                        }
                    }
                }
            },
            pluginChange: function(i) {
                var p = i.target.options[i.target.selectedIndex].value;
                konk.plugins.renderParams(i.target, p);
            },
            numLoaded: function() {
                return konk.util.sizeofObject(konk.plugins.plugins.in) +
                       konk.util.sizeofObject(konk.plugins.plugins.p2p) +
                       konk.util.sizeofObject(konk.plugins.plugins.out);
            }
        },
        work: {
            _inited: false,

            init: function(d) {
                if (d.splits) {
                    var row = '<tr>';
                    // draw the splits table
                    for (var i=0; i<d.job.M; i++) {
                        if (i > 0 && (i % 10 == 0)) {
                            row += '</tr>';
                            $('#splitsVisual').append(row);
                            row = '<tr>';
                        }
                        row += '<td id="split' + i + '" class="empty"></td>';
                    }
                    row += '</tr>';
                    $('#splitsVisual').append(row);
                }
                if (d.partitions) {
                    var row = '<tr>';
                    for (var i=0; i<d.job.M; i++) {
                        if (i > 0 && (i % 10 == 0)) {
                            row += '</tr>';
                            $('#partitionsVisual').append(row);
                            row = '<tr>';
                        }
                        row += '<td id="partition' + i + '" class="empty"></td>';
                    }
                    row += '</tr>';
                    $('#partitionsVisual').append(row);

                    konk.work._inited = true;
                }
            },

            render: function(d) {
                if (!konk.work._inited) {
                    konk.work.init(d);
                }

                var n = 0;
                if (d) {
                    $('#splitsVisual td').attr('class', 'empty');
                    if (d.splits) {
                        $('#splits .queued .value').html(konk.util.sizeofObject(d.splits.queued));
                        $('#splits .done .value').html(konk.util.sizeofObject(d.splits.done));
                        if (d.splits.queued) {
                            for (var i in d.splits.queued) {
                                $('#split' + i).attr('class', 'queued');
                            }
                        }
                        if (d.splits.done) {
                            for (var i in d.splits.done) {
                                var a = 0;
                                var u = 0;
                                var x = 0;
                                for (var w in d.splits.done[i]) {   
                                    switch(d.workers[d.splits.done[i][w]].status) {
                                        case 'available':
                                            a++;
                                            break;
                                        case 'unavailable':
                                            u++;
                                            break;
                                        case 'dead':
                                            x++;
                                            break;
                                    }
                                }
                                if (a > 0) {
                                    $('#split' + i).attr('class', 'done-available');
                                }
                                else if (u > 0) {
                                    $('#split' + i).attr('class', 'done-unavailable');
                                }
                                else {
                                    $('#split' + i).attr('class', 'done-dead');
                                }
                            }
                        }
                    }

                    $('#partitionsVisual td').attr('class', 'empty');
                    if (d.partitions) {
                        $('#partitions .queued .value').html(konk.util.sizeofObject(d.partitions.queued));
                        if (d.partitions.queued) {
                            for (var i in d.partitions.queued) {
                                $('#partition' + i).attr('class', 'queued');
                            }
                        }
                        if (d.partitions.done) {
                            var doneTotal = 1;
                            for (var i in d.partitions.done) {
                                doneTotal += (d.partitions.done[i][1] - d.partitions.done[i][0]);
                            }
                            $('#partitions .done .value').html(doneTotal);

                            for (var i in d.partitions.done) {
                                var pair = d.partitions.done[i];
                                for (var j=pair[0]; j<=pair[1]; j++) {
                                    $('#partition' + j).attr('class', 'done');
                                }
                            }
                        }
                    }
                }
            }
        },
        workers: {
            t: null,

            render: function(d) {
                if (konk.workers.t == null) {
                    konk.workers.t = $('#workers .template').clone();
                    $('#workers .template').remove();
                }
                $('#workers tbody').empty();
                var n = 0;
                if (d) {
                    for (var i in d) {
                        n++;
                        var row = konk.workers.t.clone();
                        row.addClass(d[i].status);
                        row.find('.connectTime').html(d[i].connectTime);
                        row.find('.status').html(d[i].status);
                        row.find('.url').html(d[i].url);
                        $('#workers tbody').append(row);
                    }
                }
                $('#workerCount').html(n); 
            }
        },
        jobs: {
            t: null,

            getInputCode: function() {
                var pluginId = $('#inputPlugin').val();
                var src = konk.plugins.plugins.in;
                var paramPrefix = 'in_';
                var paramName = 'inputParams';

                return konk.jobs._getCode(pluginId, src, paramPrefix, paramName);
            },

            getP2pCode: function() {
                var pluginId = $('#p2pPlugin').val();
                var src = konk.plugins.plugins.p2p;
                var paramPrefix = 'p2p_';
                var paramName = 'p2pParams';

                return konk.jobs._getCode(pluginId, src, paramPrefix, paramName);
            },

            getOutputCode: function() {
                var pluginId = $('#outputPlugin').val();
                var src = konk.plugins.plugins.out;
                var paramPrefix = 'out_';
                var paramName = 'outputParams';

                return konk.jobs._getCode(pluginId, src, paramPrefix, paramName);
            },
            _getCode: function(pluginId, src, paramPrefix, paramName) {
                var params = {};
                
                if (pluginId == 'null') {
                    return null;
                }

                for (var i in src[pluginId][PARAMS]) {
                    var v = $('#' + paramPrefix + i).val();
                    if (v == null || v == '') {
                        v = src[pluginId][PARAMS][i];
                    }
                    params[i] = v;
                }
                
                var code = src[pluginId][CODE];
                code += "\n" + 'var ' + paramName + ' = ' + JSON.stringify(params);
                return code;
            },

            active: {
                t: null,

                remove: function(id) {
                    if (confirm("Are you sure you want to remove this job (" + id + ")?")) {
                        var m = konk.createMessage(konk.TYPE_REMOVEJOB, {
                            id: id
                        });
                        konk.log(m);
                        konk.sendMessage(m);
                    }
                },

                render: function(d) {
                    if (konk.jobs.active.t == null) {
                        konk.jobs.active.t = $('#activeJob .template').clone();
                        $('#activeJob .template').remove();
                    }
                    $('#activeJob tbody').empty();
                    var n = 0;
                    if (d) {
                        n++;
                        var row = konk.jobs.active.t.clone();
                        row.find('.jobId').html(d.jobId);
                        row.find('.R').html(d.R);
                        row.find('.M').html(d.M);
                        row.find('.code pre').html(d.code);
                        row.find('.startTime').html(d.startTime);
                        row.find('.tools .rm').bind('click', function() {
                            konk.jobs.active.remove(d.jobId);
                            return false;
                        });
                        $('#activeJob').append(row);
                    }
                    if (d && d.finished) {
                        $('#activeJobCount').html(n + ' finished'); 
                    }
                    else {
                        $('#activeJobCount').html(n + ' running'); 
                    }
                }
            },
            queue: {
                t: null,

                add: function() {
                    var inCode = konk.jobs.getInputCode();
                    var p2pCode = konk.jobs.getP2pCode();
                    var outCode = konk.jobs.getOutputCode();
                    if (inCode == null) {
                        alert('Please select an input plugin');
                        return;
                    }
                    if (p2pCode == null) {
                        alert('Please select a p2p plugin');
                        return;
                    }
                    if (outCode == null) {
                        alert('Please select an output plugin');
                        return;
                    }
                    var code = $('#jobCode').val();
                    code += "\n";
                    code += inCode;
                    code += "\n";
                    code += p2pCode;
                    code += "\n";
                    code += outCode;
                    konk.log(code);
                    var m = konk.createMessage(konk.TYPE_ADDJOB, {
                        code: code,
                        R: $('#R').val(),
                        M: $('#M').val(),
                        heartbeatTimeout: $('#heartbeatTimeout').val(),
                        progressTimeout: $('#progressTimeout').val()
                    });
                    konk.log(m);
                    konk.sendMessage(m);
                },

                remove: function(id) {
                    if (confirm("Are you sure you want to remove this job (" + id + ")?")) {
                        var m = konk.createMessage(konk.TYPE_REMOVEJOB, {
                            id: id
                        });
                        konk.log(m);
                        konk.sendMessage(m);
                    }
                },

                render: function(d) {
                    if (konk.jobs.queue.t == null) {
                        konk.jobs.queue.t = $('#jobQueue .template').clone();
                        $('#jobQueue .template').remove();
                    }
                    $('#jobQueue tbody').empty();
                    var n = 0;
                    if (d) {
                        for (var i in d) {
                            n++;
                            var row = konk.jobs.queue.t.clone();
                            row.find('.jobId').html(d[i].jobId);
                            row.find('.R').html(d[i].R);
                            row.find('.M').html(d[i].M);
                            row.find('.code pre').html(d[i].code);
                            row.find('.tools .rm').bind('click', function() {
                                konk.jobs.queued.remove(d[i].jobId);
                                return false;
                            });
                            $('#jobQueue').append(row);
                        }
                    }
                    $('#jobQueueCount').html(n); 
                }
            },
            history: {
                t: null,

                remove: function(id) {
                    if (confirm("Are you sure you want to remove this job (" + id + ")?")) {
                        var m = konk.createMessage(konk.TYPE_REMOVEJOB, {
                            id: id
                        });
                        konk.log(m);
                        konk.sendMessage(m);
                    }
                },

                render: function(d) {
                    if (konk.jobs.history.t == null) {
                        konk.jobs.history.t = $('#jobHistory .template').clone();
                        $('#jobHistory .template').remove();
                    }
                    $('#jobHistory tbody').empty();
                    var n = 0;
                    if (d) {
                        for (var i in d) {
                            n++;
                            var row = konk.jobs.history.t.clone();
                            row.jobId = d[i].jobId;
                            row.find('.jobId').html(d[i].jobId);
                            row.find('.R').html(d[i].R);
                            row.find('.M').html(d[i].M);
                            row.find('.code pre').html(d[i].code);
                            row.find('.startTime').html(d[i].startTime);
                            row.find('.finishTime').html(d[i].finishTime);
                            row.find('.tools .rm').bind('click', function() {
                                konk.jobs.history.remove(row.jobId);
                                return false;
                            });
                            $('#jobHistory').append(row);
                        }
                    }
                    $('#jobHistoryCount').html(n); 
                }
            }
        },
        createMessage: function(type, spec) {
            var ret = {
                payload: {}
            };
            ret.type = type;
            for (var p in spec) {
                if (spec.hasOwnProperty(p)) {
                    ret.payload[p] = spec[p];
                }
            }
            return ret;
        },
        sendMessage: function(msg) {
            konk.ws.send(JSON.stringify(msg));
        },
        log: function(s) {
            //$('#log').prepend('<p>' + konk.util.esc(s) + '</p>');
            if (typeof(console) != 'undefined') {
                console.log(s);
            }
        },
        util: {
            esc: function(s) {
                return s.replace(/&/g, '&amp;')
                        .replace(/</g, '&lt;')
                        .replace(/>/g, '&gt;');
            },
            sizeofObject: function(o) {
                var n = 0;
                if (typeof(o) != 'undefined') {
                    for (var p in o) {
                        if (o.hasOwnProperty(p)) {
                            n++;
                        }
                    }
                }
                return n;
            }
        }
    }
})();
window.addEventListener('load', konk.init, false);

