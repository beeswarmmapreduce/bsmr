/**
 konk-ui.js

 BrowserSocket MapReduce
 Optional UI related elements and overrides
 
 Konrad Markus <konker@gmail.com>

 TODO:
 */


if (typeof(konk) != 'undefined') {
    // override _autostart
    konk._autostart = false;
    konk.__log_cnt = 0;

    // add a visual log
    var oldlog = konk.log;
    konk.log = function(s, level) {
        ++konk.__log_cnt;

        if (!level) {
            level = 'log';
        }
        if (level == 'log') {
             $('#console').prepend('<p id="l' + konk.__log_cnt + '" class="' + konk.util.esc(level) + '"><a href="#l' + (konk.__log_cnt+1) + '">U</a> | <a href="#l' + (konk.__log_cnt-1) + '">D</a> <span class="level">' + konk.util.esc(level) + '</span> <span class="msg">' + konk.util.esc(s + '') + '</span></p>');
        }
        else {
            if (typeof(s) == 'object') {
                if (level == 'w' && s.type == 'HB') {
                    level = 'h';
                }
                if (level == 'm' && s.type == 'CTL') {
                    level = 'c';
                }
                if (typeof(s.payload) != 'undefined') {
                    if (typeof(s.payload.data) != 'undefined') {
                        //s.payload.data = '[data]';
                    }
                }
                s = JSON.stringify(s, null, '  ');
            }
            $('#console').prepend('<p id="l' + konk.__log_cnt + '" class="' + konk.util.esc(level) + '"><a href="#l' + (konk.__log_cnt+1) + '">U</a> | <a href="#l' + (konk.__log_cnt-1) + '">D</a> <span class="level">' + konk.util.esc(level) + '</span></p><pre class="msg">' + konk.util.esc(s) + '</pre>');
        }
        oldlog(s, level);
    }
    konk.info = function() {
        $('#info').html('<p>MASTER: ' + konk.MASTER_WS_URL + '</p>');
    }
}

konkui = (function() {
    return {
        init: function() {
            $('#control .init').bind('click', function() {
                konk.init();

                // these should ideally be done on some kind of 'init' event?
                $('#modeForm :radio').removeAttr('disabled');
                $('#modeForm').removeClass('disabled');
                return false;
            });
            $('#control .start').bind('click', function() {
                konk.start();
                return false;
            });
            $('#control .step').bind('click', function() {
                konk.step();
                return false;
            });
            $('#control .stop').bind('click', function() {
                konk.stop();
                return false;
            });

            $('#mode_nor').val(konk.MODE_NOR).attr('checked', 'checked');
            $('#mode_ive').val(konk.MODE_IVE);
            $('#mode_tmo').val(konk.MODE_TMO);

            $('#modeForm :radio').bind('change', function() {
                konkui.control.setMode($('#modeForm :radio:checked').val());
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
                var m = konk.createMessage(konk.TYPE_CTL, {
                    action: 'mode',
                    mode: parseInt(m)
                });
                konk.worker.sendMessage(m);
            }
        }
    }
})();

$(function() {
    konk.info();
    konkui.init();
});
