/**
 bsmr-ui.js

 BrowserSocket MapReduce
 Optional UI related elements and overrides
 
 Konrad Markus <konker@gmail.com>

 TODO:
 */


if (typeof(bsmr) != 'undefined') {
    // override _autostart
    bsmr._autostart = false;

    // add a visual log
    var oldlog = bsmr.log;
    bsmr.log = function(s, level) {
        if (!level) {
            level = 'log';
        }
        if (level == 'log') {
             $('#console').prepend('<p class="' + bsmr.util.esc(level) + '"><span class="level">' + bsmr.util.esc(level) + '</span> <span class="msg">' + bsmr.util.esc(s + '') + '</span></p>');
        }
        else {
            if (typeof(s) == 'object') {
                if (level == 'w' && s.type == 'HB') {
                    level = 'h';
                }
                if (level == 'm' && s.type == 'CTL') {
                    level = 'c';
                }
                s = JSON.stringify(s, null, '  ');
            }
            $('#console').prepend('<p class="' + bsmr.util.esc(level) + '"><span class="level">' + bsmr.util.esc(level) + '</span></p><pre class="msg">' + bsmr.util.esc(s) + '</pre>');
        }
        oldlog(s, level);
    }
    bsmr.info = function() {
        $('#info').html('<p>MASTER: ' + bsmr.MASTER_WS_URL + '</p>');
    }
}

bsmrui = (function() {
    return {
        init: function() {
            $('#control .init').bind('click', function() {
                bsmr.init();

                // these should ideally be done on some kind of 'init' event?
                $('#modeForm :radio').removeAttr('disabled');
                $('#modeForm').removeClass('disabled');
                return false;
            });
            $('#control .start').bind('click', function() {
                bsmr.start();
                return false;
            });
            $('#control .step').bind('click', function() {
                bsmr.step();
                return false;
            });
            $('#control .stop').bind('click', function() {
                bsmr.stop();

                // these should ideally be done on some kind of 'stop' event?
                $('#modeForm :radio').attr('disabled', 'disabled');
                $('#modeForm').addClass('disabled');
                return false;
            });

            $('#mode_nor').val(bsmr.MODE_NOR);
            $('#mode_ive').val(bsmr.MODE_IVE).attr('checked', 'checked');;
            $('#mode_tmo').val(bsmr.MODE_TMO);

            $('#modeForm :radio').bind('change', function() {
                bsmrui.control.setMode($('#modeForm :radio:checked').val());
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
                var m = bsmr.createMessage(bsmr.TYPE_CTL, {
                    action: 'mode',
                    mode: parseInt(m)
                });
                bsmr.worker.sendMessage(m);
            }
        }
    }
})();

$(function() {
    bsmr.info();
    bsmrui.init();
});
