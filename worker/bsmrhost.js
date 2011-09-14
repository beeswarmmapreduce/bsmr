
if (typeof(importScripts) == typeof(undefined)) {
    // Mimes webworker importScripts function. Respects import order, but does not block!
    var importScripts = function() {
        var args = Array.prototype.slice.call(arguments);
        var filename = args.shift();
        if (!filename) {
            return;
        }
        var addQuotes = function(s) {
            return '"' + s + '"'; 
        }
        var quoted = Array.prototype.map.call(args, addQuotes);
        var newargs = quoted.join(',');
        var tag = "<script type='text/javascript' src='" + filename + "' onload='importScripts(" + newargs + ")'></scr" + "ipt>";
        document.write(tag);
    }
}

importScripts("console.js",
              "localstore.js",
              "generator.js", //convenience
              "iengine.js",
              "debuginput.js", //convenience
              "output.js",
              "inter.js",
              "rengine.js",
              "rcore.js",
              "cengine.js",
              "mengine.js",
              "job.js",
              "worker.js",
              "main.js");

