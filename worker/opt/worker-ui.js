/**
 worker-ui.js

 BrowserSocket MapReduce
 Optional UI related elements and overrides
 
 Konrad Markus <konker@gmail.com>

 TODO:
 */


if (typeof(worker) != 'undefined') {
    // override _autostart
    worker._autostart = true;
    worker.__log_cnt = 0;

    // add a visual log
    var oldlog = worker.log;
    worker.log = function(s, type, level) {
        if (level <= worker._loglevel) {
            ++worker.__log_cnt;

            if (!type) {
                type = 'log';
            }
            if (type == 'log') {
                 $('#console').prepend('<p id="l' + worker.__log_cnt + '" class="' + worker.util.esc(type) + '"><a href="#l' + (worker.__log_cnt+1) + '">U</a> | <a href="#l' + (worker.__log_cnt-1) + '">D</a> <span class="type">' + worker.util.esc(type) + '</span> <span class="msg">' + worker.util.esc(s + '') + '</span></p>');
            }
            else {
                if (typeof(s) == 'object') {
                    if (type == 'w' && s.type == 'HB') {
                        type = 'h';
                    }
                    if (type == 'm' && s.type == 'CTL') {
                        type = 'c';
                    }
                    if (typeof(s.payload) != 'undefined') {
                        if (typeof(s.payload.data) != 'undefined') {
                            //s.payload.data = '[data]';
                        }
                    }
                    s = JSON.stringify(s, null, '  ');
                }
                $('#console').prepend('<p id="l' + worker.__log_cnt + '" class="' + worker.util.esc(type) + '"><a href="#l' + (worker.__log_cnt+1) + '">U</a> | <a href="#l' + (worker.__log_cnt-1) + '">D</a> <span class="type">' + worker.util.esc(type) + '</span></p><pre class="msg">' + worker.util.esc(s) + '</pre>');
            }
            oldlog(s, type, level);
        }
    }
    worker.info = function() {
        $('#info').html('<p>MASTER: ' + worker.MASTER_WS_URL + '</p>');
        if (worker.server.bs) {
            $('#info').append('<p>BS: ' + worker.server.bs.port + ', ' + worker.server.bs.resourcePrefix + '</p>');
        }
    }
}

workerui = (function() {
    return {
        init: function() {
            $('#control .init').bind('click', function() {
                worker.init();

                // these should ideally be done on some kind of 'init' event?
                $('#modeForm :radio').removeAttr('disabled');
                $('#modeForm').removeClass('disabled');
                return false;
            });
            $('#control .start').bind('click', function() {
                worker.start();
                return false;
            });
            $('#control .step').bind('click', function() {
                worker.step();
                return false;
            });
            $('#control .stop').bind('click', function() {
                worker.stop();
                return false;
            });

            $('#mode_nor').val(worker.MODE_NOR).attr('checked', 'checked');
            $('#mode_ive').val(worker.MODE_IVE);
            $('#mode_tmo').val(worker.MODE_TMO);

            $('#modeForm :radio').bind('change', function() {
                workerui.control.setMode($('#modeForm :radio:checked').val());
            });
        },
        control: {
            init: function() {
            },
            start: function() {
            },
            step: function() {
            },
            stop: function() {
            },

            // send a mode ctrl message to the worker
            setMode: function(m) {
                var m = worker.createMessage(worker.TYPE_CTL, {
                    action: 'mode',
                    mode: parseInt(m)
                });
                worker.worker.sendMessage(m);
            },

            setLogLevel: function(l) {
                worker._loglevel = l;
            },

            setReduceMode: function(m) {
                worker._reducemode = m;
            }
        }
    }
})();

$(function() {
    worker.info();
    workerui.init();
});
