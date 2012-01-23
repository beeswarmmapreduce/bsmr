// The class that implements a simple request-response protocol for exchanging data using
// FlashP2P


/*
The simple JSON request-response protocol for bsmr, examples of the message types

{
operation: "request",
jobId: 2313,
splitId: 24,
bucketId: 11
}

{
operation: "response",
split_id: 24,
bucket_id: 11,
data: "ihwao98fh292hf9h84293hf4298h298h4f"
}


{
operation: "not_found",
split_id: 24,
bucket_id: 11
}


*/

var flashP2P;

function FlashCommunicator()
   {
   var self = this;
   
   flashP2P = new FlashP2P();
   
   var connectedPeers = new Object();
   var outgoingQueue = new Object();			//if no connection to a peer, the message gets queued here 
  
   var responseListeners = new Array();
   var requestListeners = new Array();
   var errorListeners = new Array();
   var idChangeListeners = new Array();
   var notFoundListeners = new Array();
   
   var connectedToCirrus = false;
   
   //Public interface that BSMR's FlashInter calls
   
   this.sendRequest = function(peerId, jobId, splitId, bucketId)
   		{
	    queueRequestMessage(peerId, jobId, splitId, bucketId);
	   	
	    if (connectedPeers[peerId])
	   		{
	   		sendQueuedRequestMessages(peerId);
	   		}
	    
	    else if (connectedToCirrus)
	   		{
	   		connect(peerId);
	   		}
	   
   		}
   
   this.sendResponse = function(peerId, jobId, splitId, bucketId, data)
   		{
	   	var response = createResponseMessage(jobId, splitId, bucketId, data);
	   	sendMessage(peerId, response);	
  		}
  
   
   this.sendNotFound = function(peerId, jobId, splitId, bucketId)
		{
	   	console.log("FlashCommunicator::sendNotFound()");
	   	var message = createNotFoundMessage(jobId, splitId, bucketId);
	   	sendMessage(peerId, message);	
		}
   
   // Public interface for adding listeners 
   
   this.addResponseListener = function(listener)
   		{
	   	responseListeners.push(listener);
   		}
   
   this.addRequestListener = function(listener)
		{
	   	requestListeners.push(listener);
		}
   
   this.addErrorListener = function(listener)
		{
	   	errorListeners.push(listener);
		}
   
   this.addIdChangeListener = function(listener)
		{
	   	idChangeListeners.push(listener);
		}
   
   this.addNotFoundListener = function(listener)
		{
	   	notFoundListeners.push(listener);
		}
   //Private helper functions
   
   
   var createRequestMessage = function(jobId, splitId, bucketId)
   		{
	   	var request = new Object();
	   	request.operation = "request";
	   	request.jobId = jobId;
	   	request.splitId = splitId;
	   	request.bucketId =bucketId;
   		return request;
   		}
   
   var createResponseMessage = function(jobId, splitId, bucketId, data)
   		{
		var response = new Object();
		response.operation = "response";
		response.jobId = jobId;
		response.splitId = splitId;
		response.bucketId = bucketId;
   		response.data = data;
		return response;
   		}
   
   var createNotFoundMessage = function(jobId, splitId, bucketId)
		{
	   	var message = new Object();
	   	message.operation = "notFound";
	   	message.jobId = jobId;
	   	message.splitId = splitId;
	   	message.bucketId =bucketId;
		return message;
		}
   
   var queueRequestMessage = function(peerId, jobId, splitId, bucketId)
   		{
	   	if (!outgoingQueue[peerId])
	   		{
	   		outgoingQueue[peerId] = new Array();
	   		}
	   	
	   	var request = createRequestMessage(jobId, splitId, bucketId); 
	   	outgoingQueue[peerId].push(request);
   		}
   
   var sendQueuedRequestMessages = function(peerId) 
   		{
	   	if (outgoingQueue[peerId])
	   		{
	   		var requests = outgoingQueue[peerId];
	   		
	   		while (true)
	   			{
	   			var request = requests.pop();
	   			if (request==null || typeof(request) == typeof(undefined))
	   				break;
	   			
	   			sendMessage(peerId, request);
	   			}		
	   		}
   		}
   
   
 
   //This can only be called after the peer with peerId is connected
   
   var sendMessage = function(peerId, message)
		{
	   if (!connectedPeers[peerId])
		   return;
	
	   flashP2P.send(peerId, JSON.stringify(message));
		}
   
   var connectToCirrus = function()
   		{
   		flashP2P.connectToCirrus("rtmfp://p2p.rtmfp.net/ac5e767303706ffbc314d1e3-7b419bc16675/");
   		}
  
   var listen = function()
   		{
   		console.log("FlashCommunicator::listen()\r\n");
   		flashP2P.listen();
   		}
   
   var connect = function(peerId)
		{
		console.log("FlashCommunicator::connect()"+"\r\n");
		flashP2P.connect(peerId);
		}


   
   //Callbacks to be called  by FlashP2P
   
   this.onCirrusConnectionStatus = function(ownId, status, statusCode, statusLevel)
   		{
   		console.log("FlashCommunicator::onCirrusConnectionStatus() "+ ownId+" "+status+"\r\n");
   		
   		connectedToCirrus = true;
   
   		var listeners = idChangeListeners;
		for (var i=0; i<listeners.length; i++)
    		{
    		listeners[i](ownId);
    		}
		
		for (var i in outgoingQueue)
			{
			connect(i); 
			}
		
		listen();
   		}		
  
  
   this.onDataArrived = function(peerId, data)
   		{
   		// A message has arrived from a remote peer whose peer id is peerID
	    // This function will decode the message and act accordingly
	   	
	   	console.log("FlashCommunicator::onDataArrived(), peerId: "+peerId+" data: "+data+"\r\n")
   
   		
	   	var message = JSON.parse(data);
   		
	   	if (message.operation == "request")
	   		{
	   		var listeners = requestListeners;
			for (var i=0; i<listeners.length; i++)
	    		{
	    		listeners[i](peerId, message.jobId, message.splitId, message.bucketId);
	    		}
	   		}
	   
	   	if (message.operation == "response")
   			{
	   		var listeners = responseListeners;
			for (var i=0; i<listeners.length; i++)
	    		{
	    		listeners[i](peerId, message.jobId, message.splitId,message.bucketId,message.data);
	    		}
   			}
   		
	   	if (message.operation == "notFound")
   			{
	   		var listeners = notFoundListeners;
			for (var i=0; i<listeners.length; i++)
	    		{
	    		listeners[i](peerId, message.jobId, message.splitId, message.bucketId);
	    		}
   			}
   		
   		
   		}	
   		
   		
   this.onConnectionAccepted = function(peerId)
   		{
   		//incoming connection established
	   
	   	console.log("FlashCommunicator::onConnectionAccepted(), connection from peer "+peerId+"\r\n");
   		connectedPeers[peerId] = true;
   		flashP2P.addDataListener(peerId,self.onDataArrived);
   		}
   		
   this.onPeerConnectionStatus = function(peerId, status, statusCode, statusLevel)		
   		{
   		console.log("FlashCommunicator::onPeerConnectionStatus(), connection to peer "+peerId+" "+status+" "+statusCode+"\r\n");
   		
   	
   	
  		//outgoing connection established
   		
   		if (status == flashP2P.STATUS_CONNECTED)
   			{	
   			connectedPeers[peerId] = true;
   			flashP2P.addDataListener(peerId,self.onDataArrived);
   			sendQueuedRequestMessages(peerId);
   			}
   		
   		else 
   			{
   			var listeners = errorListeners;
   			for (var i=0; i<listeners.length; i++)
   				{
   				listeners[i](status);
   				}
   			}
   		
   		}
   		
   
   //initialization	
   
   flashP2P.addCirrusConnectionListener(self.onCirrusConnectionStatus);
   flashP2P.addAcceptListener(self.onConnectionAccepted);
   flashP2P.addPeerConnectionListener(self.onPeerConnectionStatus);
   connectToCirrus();
   }
