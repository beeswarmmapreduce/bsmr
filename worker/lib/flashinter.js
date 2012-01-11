function FlashInter(job, localStorage)
{
var self = this;	

var target = null;
var peerUrls = null;
var peerUrlIndex = null;
var currentSplitId = null;
var currentPartitionId = null;
var jobId = job.id;

var flashCommunicator= new FlashCommunicator();



// Starts feeding a chunk identified by splitId, partitionId from peerUrl
// into the target 

this.feed = function(splitId, partitionId, _peerUrls, _target) 
	{
	currentSplitId = splitId;
	currentPartitionId = partitionId;
	target = _target;
    peerUrls = _peerUrls;
    peerUrlIndex = 0;
	
    flashCommunicator.sendRequest(peerUrls[peerUrlIndex], jobId, splitId, partitionId);
    }


//interface that the FlashCommunicator calls

//inner class needed for storing the peerid and buffer

function Responder(peerId)
	{
	var self = this;
	var buffer = new Array();
	
	this.write = function(splitId, partitionId, pairs, more) 
		{
    	buffer = buffer.concat(pairs);
    	if (!more) 
    		{
        	flashCommunicator.sendResponse(peerId, jobId, splitId, partitionId, buffer);
        	buffer = undefined;
        	buffer = new Array();
        	}
		}
	}

this.onRequest = function(peerId, jobId, splitId, partitionId)
	{
	if (localStorage.canhaz(splitId,partitionId))
		{
		//if we have the data
		var responder = new Responder(peerId);
		
		localStorage.feed(splitId, partitionId, responder);
		}
	
	else
		{
		flashCommunicator.sendNotFound(peerId, jobId, splitId, partitionId);
		}
	}


this.onNotFound = function(peerId, jobId, splitId, partitionId)
	{
	self.onError("Requested Data Not Found");
	}

this.onResponse = function(peerId, jobId, splitId, partitionId, data)
	{
	target.write(splitId, partitionId, data);
	}

this.onError = function(status)
	{
	dump("FlashInter::onError "+status);
	
	job.markUnreachable(peerUrls[peerUrlIndex]);
	
	peerUrlIndex++;
	
	if (peerUrlIndex >= peerUrls.legth)
		 job.onSplitFail(currentSplitId, currentPartitionId);
	else
		 flashCommunicator.sendRequest(peerUrls[peerUrlIndex], jobId, splitId, partitionId);
	
	}

this.onIdChange = function(id)
	{
	job.setOwnPeerId(id);
	}

//initialization
flashCommunicator.addRequestListener(this.onRequest);
flashCommunicator.addResponseListener(this.onResponse);
flashCommunicator.addErrorListener(this.onError);
flashCommunicator.addIdChangeListener(this.onIdChange);
flashCommunicator.addNotFoundListener(this.onNotFound);
}


//Factory method

function flashInter(job, localStorage)
	{
	return new FlashInter(job, localStorage);
	}