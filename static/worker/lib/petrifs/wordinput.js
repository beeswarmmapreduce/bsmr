function wordInput(filename) {

	var URL = 'http://localhost:8080/fs/filesystem';
    if (typeof(filename) == typeof(undefined)) {
    	console.error('wordinput error: No filename given!');
    }

    var maxlen = 10; 

    function Input(M) {
        this.M = M;
    }

    Input.prototype.computeSizes = function(splitId, target) {
        if (typeof(this.fullSize) == typeof(undefined)) {
            var input = this;
            var request = new XMLHttpRequest();
            request.open('GET', URL + '?operation=sizeof&filename=' + filename, true);  
            request.onreadystatechange = function() {  
                if (request.readyState == 4) {  
                    if(request.status == 200) {
                        var fullSize = JSON.parse(request.responseText);
                        input.fullSize = fullSize;
                        input.blockSize = Math.ceil(fullSize / input.M);
                        input.feed2(splitId, target);
                    }
                }  
            };  
            request.send(null);
        }
        else {
            this.feed2(splitId, target);
        }
    };

    Input.prototype.feed = function(splitId, target) {
        this.computeSizes(splitId, target);
    };

    Input.prototype.feed2 = function(splitId, target) {

        var lengthOK = function(x) {
            var l = x.length;
            return  0 < l && l <= maxlen;
        };
        var makepair = function(x) {
            return [filename, x];
        };
        var feedWords = function(splitId, text, extra) {
            var words = text.split(/\s+/);
            var last = words.pop();
            var next = extra.split(/\s+/).shift();
            words.push(last + next);
            words.shift(); //discard first item
            var shrt = words.filter(lengthOK);
            var pairs = shrt.map(makepair);
            target.write(splitId, pairs);
        };
        var blockSize = this.blockSize;
        var begin = splitId * this.blockSize;
        var wanted = blockSize + maxlen;
        var left = this.fullSize - begin;
        var length = Math.min(wanted, left);

        var request = new XMLHttpRequest();  
        request.open('GET', URL + '?operation=read&filename=' + filename + '&begin=' + begin + '&length=' + length, true);  
        request.onreadystatechange = function() {  
            if (request.readyState == 4) {  
                if(request.status == 200) {
                    var raw = request.responseText;
                    var prefix = splitId == 0 ? " " : "";
                    var text = prefix + raw.slice(0, blockSize);
                    var extra = raw.slice(blockSize, wanted);
                    feedWords(splitId, text, extra);
                }
            }  
        };  
        request.send(null);  
    };
    
	var factory = function(M) {
	    return new Input(M);
	};
	
	return factory;
}
   
