
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

importScripts("console.js",
              "lib/generator.js",
              "lib/boringbucket.js",
              "lib/flashinter.js",
              "lib/nointer.js",
              "lib/debuginput.js",
              "lib/consoleout.js",
              "lib/nointer.js",
              "lib/wordinput.js",
              "localstore.js",
              "iengine.js",
              "rengine.js",
              "rcore.js",
              "cengine.js",
              "mengine.js",
              "job.js",
              "worker.js",
              "main.js");

