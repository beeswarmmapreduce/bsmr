
/* this is a hack that makes debugging logging from inside a webworker possible */

if (typeof(console) == typeof(undefined)) {
    var console = {
      "log": function(msg) {
        self.postMessage(msg);
      }
    };
}

