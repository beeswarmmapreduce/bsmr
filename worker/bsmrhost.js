"use strict";

// Mimes webworker importScripts function.
// Respects import order, but does not block!
function myImport() {
	var args = Array.prototype.slice.call(arguments);
    var filename = args.shift();
    if (typeof(filename) == typeof(undefined)) {
        return;
    }
    var preImport = function(args){myImport.apply(null, args);};
    var next = function(){preImport(args);};
    var tag = document.createElement('script');
    tag.type = 'text/javascript';
    tag.src = filename;
    tag.onreadystatechange = next;
    tag.onload = next;
    var head = document.getElementsByTagName('head')[0];
    head.appendChild(tag);
};

var extras = [];
var library = ["lib/boringbucket.js",
               "lib/generator.js",
               "lib/debug/consoleout.js",
               "lib/debug/debuginput.js",
               "lib/debug/nointer.js",
               "lib/petrifs/petrifsout.js",
               "lib/petrifs/wordinput.js"];

var host = ["host/localstore.js",
            "host/chunkregistrar.js",
            "host/rcore.js",
            "host/rtask.js",
            "host/iengine.js",
            "host/rengine.js",
            "host/cengine.js",
            "host/mengine.js",
            "host/job.js",
            "host/worker.js",
            "host/main.js"];

var flash = ["external/swfobject.js",
             "lib/flashinter/flashhelper.js",
             "lib/flashinter/flashp2p.js",
             "lib/flashinter/flashcommunicator.js",
             "lib/flashinter/flashinter.js"];

extras = extras.concat(flash);

var imports = extras.concat(library).concat(host);

myImport.apply(this, imports);

