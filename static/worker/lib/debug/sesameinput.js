function sesameInput() {
  // The input method factory creator.
  // Handles all parameters given from the job code.
  // The parameters would typically contain input file names and such.
  // Returns a factory for constructing input handlers.

  function Input(M) {
    // Input handler constructor.
    // Takes the amount of splits (M) as argument.
    this.M = M;
  }

  Input.prototype.feed = function(splitId, target) {
    // Input handler public method feed
    // Takes care of generating or retrieving all data related to
    // the given splitId (< M) and writing the corresponding
    // entries to the given target component.

    var data = ['one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight'];
    for (var i in data) {

      // example distribution
      var splitId_of_ith_element = i % this.M;

      if (splitId_of_ith_element == splitId) {

        // key is usually a filename
        var key = 'data'

        var value = data[i];
        var pair = [key, value];

        // With a real input source we might prefer
        // writing several entries in one go.
        var some = [pair];

        var gotMore = true;
        target.write(splitId, some, gotMore);
      }
    }

    // The last write does not need to be empty,
    // but it does need to signal gotMore as false.
    target.write(splitId, [], false);
  };

  // we can define an input handler factory as follows

  var factory = function(M) {
    return new Input(M);
  };

  return factory;
}
   
