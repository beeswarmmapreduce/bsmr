/**
    worker-engine.js


 */

var engine = (function() {
    return {
        TYPE_LOG: "LOG",
        TYPE_ENG: "ENG",

        map: {
            job: null,
            result: [],

            reset: function(job) {
                engine.map.job = job;
                engine.map.result = [];
            },

            hash: function(k) {
                var n = 0;
                for (var i=0; i<k.length; i++) {
                    n += k.charCodeAt(i);
                }
                return (n % engine.map.job.R);
            },
            emit: function(k, v) {
                try {
                    var h = engine.map.hash(k);
                    if (typeof(engine.map.result[h]) == 'undefined') {
                        engine.map.result[h] = {};
                    }
                    if (typeof(engine.map.result[h][k]) == 'undefined') {
                        engine.map.result[h][k] = [];
                    }
                    engine.map.result[h][k].push(v);
                }
                catch(ex) {
                    engine.log('MAP FAILED: ' + k + ', ' + h + ', ' + v);
                    engine.log(ex + '');
                }
            },
            exec: function(spec) {
                engine.map.reset(spec.job);

                // eval and execute the code against the data
                eval(spec.job.code);
                engine.log('MAP FUNCTION: ' + map);
                map('k1', spec.data, engine.map.emit);

                // send back the result to the parent worker
                var m = engine.createMessage(engine.TYPE_ENG, {
                    action: "map",
                    job: spec.job,
                    splitId: spec.splitId,
                    data: engine.map.result
                });
                postMessage(m);
            }
        },
        reduce: {
            job: null,
            result: {},

            reset: function(job) {
                engine.reduce.job = job;
                engine.reduce.result = {};
            },

            emit: function(k, v) {
                engine.reduce.result[k] = v;
            },
            exec: function(spec) {
                engine.reduce.reset(spec.job);

                // eval and execute the code against the data
                eval(spec.job.code);
                engine.log('REDUCE FUNCTION: ' + reduce);
                for (var k in spec.data) {
                    var _emit = function(v) {
                        engine.reduce.emit(k, v);
                    }
                    reduce(k, spec.data[k], _emit);
                }

                // send back the result to the parent worker
                var m = engine.createMessage(engine.TYPE_ENG, {
                    action: "reduce",
                    job: spec.job,
                    partitionId: spec.partitionId,
                    splitId: spec.splitId,
                    data: engine.reduce.result
                });
                postMessage(m);
            }
        },

        log: function(s) {
            var m = engine.createMessage(engine.TYPE_LOG, {
                message: s
            });
            postMessage(m);
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
        }
    }
})();

/* receive incoming message from parent */
function onmessage(msg) {
    switch (msg.data.payload.action) {
        case 'map':
            engine.map.exec(msg.data.payload);
            break;
        case 'reduce':
            engine.reduce.exec(msg.data.payload);
            break;
        default:
            // swallow for now
    }
}


function _gP(o) {
    var s = '[';
    for (var p in o) {
        s += p + '->' + o[p] + ', ';
    }
    return (s + ']');
}

