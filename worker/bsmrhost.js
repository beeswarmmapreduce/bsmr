"use strict";

var importScripts = function(){
	// Mimes webworker importScripts function. Respects import order, but does not block!
	var myImport = function() {
		var args = Array.prototype.slice.call(arguments);
	    var filename = args.shift();
	    if (typeof(filename) == typeof(undefined)) {
	        return;
	    }
	    var next = function(){myImport.apply(null, args);};
	    var tag = document.createElement('script');
	    tag.type = 'text/javascript';
	    tag.src = filename;
	    tag.onreadystatechange = next;
	    tag.onload = next;
	    var head = document.getElementsByTagName('head')[0];
	    head.appendChild(tag);
	}
    if (typeof(importScripts) == typeof(undefined)) {
    	return myImport;
    }
    return importScripts;
}();

var basics = ["console.js"];
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

// add extras that do not work inside webworkers
if(typeof(document) != typeof(undefined)) {
	extras = extras.concat(flash);
}

var imports = basics.concat(extras).concat(library).concat(host);

importScripts.apply(this, imports);

