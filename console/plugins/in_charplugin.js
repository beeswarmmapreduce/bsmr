params =
	{
		label : "CharPlugin",
		repeat : "1"
	};

function inputPlugin(params, onData, onError)
	{

	var alphabet = [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k',
			'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
			'y', 'z' ];
	var ret = '';

	for ( var i = 0; i < params.repeat; i++)
		{
		for ( var j = 0; j < Math.pow(2, (i + 1)); j++)
			{
			ret += alphabet[i];
			}
		worker.log('Generated: ' + Math.pow(2, (i + 1)) + ' of ' + alphabet[i],
				'log', LOG_ERROR);
		}

	this.fetchMapData = function(jobId, M, inputRef, splitId)
		{
		try
			{
			var rlen = ret.length;
			var splitLen = Math.floor(rlen / M);
			onData(jobId, splitId, ret.substr(splitId * splitLen, splitLen))
			}
		catch (ex)
			{
			onError(ex);
			}
		}
	}