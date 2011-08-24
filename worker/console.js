
if (typeof(console) == typeof(undefined)) {
    var console = {
      "log": function(msg) {
        self.postMessage(msg);
      }
    }
}

