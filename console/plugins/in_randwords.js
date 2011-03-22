params =
	{
		label : "Random Words",
		numWords : "1000",
		wordLen : "4"
	}

function inputPlugin(params, onData, onError)
	{
	function randChar()
		{
		return String.fromCharCode(97 + (Math.random() * (122 - 97)));
		}
	function randWord(wordLen)
		{
		ret = "";
		for ( var i = 0; i < wordLen; i++)
			{
			ret += randChar();
			}
		return ret;
		}
	function randString(numWords, wordLen)
		{
		var ret = "";
		for ( var i = 0; i < numWords; i++)
			{
			ret += randWord(wordLen);
			ret += " ";
			}
		return ret;
		}

	this.rep = randString(parseInt(params['numWords'], 10), parseInt(
			params['wordLen'], 10));

	this.fetchMapData = function(jobId, M, inputRef, splitId)
		{
		try
			{
			var rlen = this.rep.length;
			var splitLen = Math.floor(rlen / M);
			onData(jobId, splitId, this.rep
					.substr(splitId * splitLen, splitLen))
			}
		catch (ex)
			{
			onError(ex);
			}
		}
	}
