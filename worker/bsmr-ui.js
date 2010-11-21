/**
   BrowserSocket MapReduce
   Optional UI related elements and overrides
 */


if (typeof(bsmr) != 'undefined') {
    // override _autoload
    bsmr._autoload = false;

    // add a visual log
    var oldlog = bsmr.log;
    bsmr.log = function(s, level) {
        var e = document.getElementById('console');
        if (e) {
            if (!level) {
                level = 'log';
            }
            if (level == 'log') {
                e.innerHTML += '<p class="' + bsmr.util.esc(level) + '"><span class="level">' + bsmr.util.esc(level) + '</span> <span class="msg">' + bsmr.util.esc(s + '') + '</span></p>'; 
            }
            else {
                if (typeof(s) == 'object') {
                    if (level == 'w' && s.type == 'HB') {
                        level = 'h';
                    }
                    s = JSON.stringify(s, null, '  ');
                }
                e.innerHTML += '<p class="' + bsmr.util.esc(level) + '"><span class="level">' + bsmr.util.esc(level) + '</span></p><pre class="msg">' + bsmr.util.esc(s) + '</pre>'; 
            }
        }
        oldlog(s, level);
    }
    bsmr.info = function() {
        var e = document.getElementById('info');
        if (e) {
            e.innerHTML += '<p>MASTER: ' + bsmr.MASTER_WS_URL + '</p>'; 
        }
    }
}
