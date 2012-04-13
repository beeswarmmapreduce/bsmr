function FlashInter(job, localStorage)
{
var self = this;	

//addFlashBinary();

var target = null;
var peerUrls = null;
var peerUrlIndex = null;
var currentSplitId = null;
var currentBucketId = null;
var jobId = job.id;

var flashCommunicator= new FlashCommunicator();

var requestTimeout = null;

// Starts feeding a chunk identified by splitId, bucketId from peerUrl
// into the target 

this.feed = function(splitId, bucketId, _peerUrls, _target) 
	{
	console.log("FlashInter::feed() "+splitId+", "+bucketId);
	currentSplitId = splitId;
	currentBucketId = bucketId;
	target = _target;
    peerUrls = _peerUrls;
    peerUrlIndex = 0;
	
    var timeoutcb = function() {self.onError('timeout');};
    requestTimeout = setTimeout(timeoutcb, 1000);
    flashCommunicator.sendRequest(peerUrls[peerUrlIndex], jobId, splitId, bucketId);
    };


//interface that the FlashCommunicator calls

//inner class needed for storing the peerid and buffer

function Responder(peerId)
	{
	var self = this;
	var buffer = new Array();
	
	this.write = function(splitId, bucketId, pairs, more) 
		{
		console.log("Responder::write(), "+splitId+", "+bucketId+", "+pairs+" ,"+more );
		console.log(pairs);		
    	buffer = buffer.concat(pairs);
    	if (!more) 
    		{
        	flashCommunicator.sendResponse(peerId, jobId, splitId, bucketId, buffer);
        	//buffer = undefined;
        	//buffer = new Array();
        	}
		};
	}

this.onRequest = function(peerId, jobId, splitId, bucketId)
	{
	console.log("FlashInter::onRequest() "+peerId+" ,"+jobId+" ,"+splitId+" ,"+bucketId);
	if (localStorage.canhaz(splitId,bucketId))
		{
		//if we have the data
		var responder = new Responder(peerId);
		
		console.log("FlashInter::onRequest(), we have the data, calling localStorage.feed()");
		localStorage.feed(splitId, bucketId, responder);
		}
	
	else
		{
		console.log("FlashInter::onRequest(), we don't have the data");
		flashCommunicator.sendNotFound(peerId, jobId, splitId, bucketId);
		}
	};


this.onNotFound = function(peerId, jobId, splitId, bucketId)
	{
	self.onError("Requested Data Not Found");
	};

this.onResponse = function(peerId, jobId, splitId, bucketId, data)
	{
	console.log("FlashInter::onResponse()");
	
	if (requestTimeout!=null)
		{
		clearTimeout(requestTimeout);
		requestTimeout = null;
		}
	
	
	target.write(splitId, bucketId, data);
	};

this.onError = function(status)
	{
	console.log("FlashInter::onError "+status);
	
	job.markUnreachable(peerUrls[peerUrlIndex]);
	
	peerUrlIndex++;
	
	if (peerUrlIndex >= peerUrls.length)
		 job.onChunkFail(currentSplitId, currentBucketId);
	else
		 flashCommunicator.sendRequest(peerUrls[peerUrlIndex], jobId, currentSplitId, currentBucketId);
	
	};

this.onIdChange = function(id)
	{
	console.log("FlashInter::onIdChange() "+id);
	job.setOwnInterUrl(id);
	};

//initialization
flashCommunicator.addRequestListener(this.onRequest);
flashCommunicator.addResponseListener(this.onResponse);
flashCommunicator.addErrorListener(this.onError);
flashCommunicator.addIdChangeListener(this.onIdChange);
flashCommunicator.addNotFoundListener(this.onNotFound);
}



function flashInter()
	{
	//Factory method
	var factory = function(job, localStorage)
		{
		return new FlashInter(job, localStorage);
		};
	return factory;
	}
