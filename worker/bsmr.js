/**
   BrowserSocket MapReduce
   worker node bootstrap and controller.
   Spawns a Web Worker thread which performs the actual work.
 */


var bsmr = (function() {
    /* the websocket url of the master to which this worker should use */
    var MASTER_WS_URL = 'ws://127.0.0.1:8080/bsmr/';

    /* debug mode on/off */
    var DEBUG = true;

    /* status codes */
    var STATUS_ERROR    = -2;
    var STATUS_STOPPED  = -1;
    var STATUS_INIT     = 1;
    var STATUS_IDLE     = 2;
    var STATUS_MAPPING  = 3;
    var STATUS_REDUCING = 4;

    /* message types */
    var TYPE_HB = 'HB';
    var TYPE_DO = 'DO';
    var TYPE_ACK = 'ACK';
    var TYPE_LOG = 'LOG';

    return {
        DEBUG: DEBUG,
        MASTER_WS_URL: MASTER_WS_URL,

        /* whether to inmmediately start work */
        _autoload: true,

        /* overall status of the worker node */
        status: null,
        //status_msgs: [],

        curJob: null,
        mapTasks: {},
        reduceTasks: {},

        init: function() {
            if (!bsmr._autoload) {
                bsmr.log('No autoload. aborting init.');
                bsmr._autoload = true;
                return;
            }

            bsmr.status = STATUS_INIT;
            //bsmr.status_msgs = [];

            if (("Worker" in window) == false) {
                bsmr.setStatus(
                    STATUS_ERROR,
                    'Your browser does not seem to support webworkers. Aborting.'
                );
                return;
            }
            if (("WebSocket" in window) == false) {
                bsmr.setStatus(
                    STATUS_ERROR,
                    'Your browser does not seem to support websockets. Aborting.'
                );
                return;
            }
            if (("BrowserSocket" in window) == false) {
                bsmr.setStatus(
                    STATUS_ERROR,
                    'Your browser does not seem to support browsersockets. Aborting.'
                );
                return;
            }

            // create a worker thread
            try {
                bsmr.worker.init();
            }
            catch (ex) {
                bsmr.setStatus(
                    STATUS_ERROR,
                    'Could not create worker thread: ' + + ex
                );
                return;
            }

            // make a ws connection to the master
            try {
                bsmr.master.init();
            }
            catch (ex) {
                bsmr.setStatus(
                    STATUS_ERROR,
                    'Could not connect to master: ' + MASTER_WS_URL + ': ' + ex
                );
                return;
            }

            // open a bs listener
            try {
                bsmr.incoming.init();
            }
            catch (ex) {
                bsmr.setStatus(
                    STATUS_ERROR,
                    'Could not open browsersocket: ' + ex
                );
            }

        },
        stop: function() {
            bsmr.master.stop();
            bsmr.worker.stop();
            bsmr.incoming.stop();
            bsmr.setStatus(
                STATUS_STOPPED,
                'stopped'
            );
        },

        /*
            web worker thread that does the actual work */
        worker: {
            thread: null,

            init: function() {
                bsmr.worker.thread = new Worker('worker.js');
                bsmr.worker.thread.onmessage = bsmr.worker.onmessage;
            },
            stop: function() {
                bsmr.worker.thread.terminate();
            },

            /* 
                if we receive a message from the worker thread,
                forward it to the master */
            onmessage: function(msg) {
                if (msg.data.type == TYPE_ACK) {
                    switch (msg.data.payload.action) {
                        case 'mapTask':
                            //[TODO: do checks and set mapTasks state]
                            bsmr.master.sendMessage(msg.data);
                            break;
                        case 'reduceSplit':
                            //[TODO: do checks and set reduceTasks state]
                            bsmr.master.sendMessage(msg.data);
                            break;
                        case 'reduceTask':
                            //[TODO: do checks and set reduceTasks state]
                            bsmr.master.sendMessage(msg.data);
                            break;
                        default:
                            bsmr.log('Unknown action received from thread: ' + msg.data.payload.action);
                    }
                }
                else if (msg.data.type == TYPE_HB) {
                    bsmr.master.sendMessage(msg.data);
                }
                else if (msg.data.type == TYPE_LOG) {
                    bsmr.log(msg.data.payload.message, 'log');
                }
            },
            sendMessage: function(msg) {
                bsmr.log(msg, 'm');
                bsmr.worker.thread.postMessage(msg);
            }
        },

        /*
            websockets connection to the master server */
        master: {
            /* the main ws channel to the master */
            ws: null,

            init: function() {
                /*[TODO: should we have a protocol here?] */
                bsmr.master.ws = new WebSocket(MASTER_WS_URL, "worker");
                bsmr.master.ws.onopen = bsmr.master.onopen;
                bsmr.master.ws.onclose = bsmr.master.onclose;
                bsmr.master.ws.onerror = bsmr.master.onerror;
            },
            stop: function() {
                bsmr.master.ws.close();
            },

            /* 
                if we receive a message from the master,
                forward it to the worker thread */
            onmessage: function(e) {
                var m = bsmr.readMessage(e.data);
                if (m.type == TYPE_DO) {
                    switch (m.payload.action) {
                        case 'mapTask':
                            //[TODO: do checks and set mapTasks state]
                            bsmr.worker.sendMessage(m);
                            break;
                        case 'reduceTask':
                            //[TODO: do checks and set reduceTasks state]
                            bsmr.worker.sendMessage(m);
                            break;
                        case 'reduceSplit':
                            //[TODO: do checks and set reduceTasks state]
                            bsmr.worker.sendMessage(m);
                            break;
                        case 'idle':
                            msmr.idle();
                            break;
                        default: 
                            // swallow?
                    }
                }
                else {
                    bsmr.log('Ignoring message of type: ' + m.type);
                }
            },
            onopen: function(e) { 
                // send the 'socket' message to master
                try {
                    var m = bsmr.createMessage(TYPE_ACK, {
                        action: 'socket',
                        url: 'ws://127.0.0.1:' + bsmr.incoming.bs.port + bsmr.incoming.bs.resourcePrefix
                    });

                    bsmr.master.sendMessage(m);
                }
                catch (ex) {
                    bsmr.setStatus(
                        STATUS_ERROR,
                        'Could not send greeting to master: ' + ex
                    );
                }

                // start accepting incoming messages from the master
                bsmr.master.ws.onmessage = bsmr.master.onmessage;

                // successful init
                bsmr.setStatus(STATUS_IDLE, 'init complete');
            },
            onclose: function(e) {
                /* FIXME: why does this cause an error? */
                bsmr.log('ws:close');
                bsmr.stop();   
            },
            onerror: function(e) {
                /*[TODO]*/
                bsmr.log('ws:error');
            },
            sendMessage: function(msg) {
                bsmr.log(msg, 'w');
                bsmr.master.ws.send(JSON.stringify(msg));
            }
        },

        /*
            browsersocket connection to other bsmr nodes */
        incoming: {
            /* the browsersocket for communicating with other worker nodes */
            bs: null,

            init: function() {
                bsmr.incoming.bs = new BrowserSocket(bsmr.incoming.handler);
                bsmr.incoming.bs.onerror = bsmr.incoming.onerror;
            },
            stop: function() {
                bsmr.incoming.bs.stop();
            },

            handler: function(req) {
                this.onmessage = function(e) {
                    /*[TODO]*/
                    bsmr.log('bs:handler:onmessage: ' + e.data);
                }
                this.onopen = function(e) { 
                    /*[TODO]*/
                    bsmr.log('bs:handler:open');
                }
                this.onclose = function(e) {
                    /*[TODO]*/
                    bsmr.log('bs:handler:close');
                }
                this.onerror = function(e) {
                    /*[TODO]*/
                    bsmr.log('bs:handler:error');
                }
            },
            onerror: function(e) {
                /*[TODO]*/
                bsmr.log('bs:error');
            }
        },

        /* utils and helpers */
        readMessage: function(s) {
            return JSON.parse(s);
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
        setStatus: function(status, msg) {
            if (msg) {
                //bsmr.status_msgs.push(msg);
                bsmr.log(msg);
            }
            bsmr.status = status;
        },
        util: {
            esc: function(s) {
                return s.replace(/&/g, '&amp;')
                        .replace(/</g, '&lt;')
                        .replace(/>/g, '&gt;');
            }
        },
        log: function(s, level) {
            if (bsmr.DEBUG) {
                if (typeof(console) != 'undefined' && typeof(console.log) == 'function') {
                    console.log(s);
                }
            }
        }
    }
})();
window.addEventListener('load', bsmr.init, false);

