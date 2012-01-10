// The class that implements a simple request-response protocol for exchanging data using
// FlashP2P


/*
The simple JSON request-response protocol for bsmr, examples of the message types

{
operation: "request",
splitId: 24,
partitionId: 11
}

{
operation: "response",
split_id: 24,
partition_id: 11,
data: "ihwao98fh292hf9h84293hf4298h298h4f"
}


{
operation: "not_found",
split_id: 24,
partition_id: 11
}


*/


function FlashCommunicator()
   {
   var self = this;
   var connectedPeers = new Object();
   var outgoingQueue = new Array();			//if no connection to a peer, the message gets queued here 
   
   var responseListeners = new Array();
   var requestListeners = new Array();
   var errorListeners = new Array();
   var idChangeListeners = new Array();
   
   //Public interface that BSMR's FlashInter calls
   
   this.sendRequest = function(peerId, splitId, partitionId)
   		{
	   	if (connectedPeers[peerId])
	   		{
	   		self.sendRequestMessage(peerId, splitId, partitionId);
	   		}
	   	
	   	else
	   		{
	   		self.queueRequestMessage(peerId, splitId, partitionId);
	   		self.connect(peerId);
	   		}
	   	
   		}
   
   this.sendResponse = function(peerId, splitId, partitionId, data)
   		{
	   
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
   
   //Private helper functions
   
   var connectToCirrus = function()
   		{
   		flashP2P.connectToCirrus("rtmfp://p2p.rtmfp.net/ac5e767303706ffbc314d1e3-7b419bc16675/");
   		}
  
   var listen = function()
   		{
   		dump("FlashCommunicator::listen()\r\n");
   		flashP2P.listen();
   		}
   
   var connect = function()
		{
		dump("FlashCommunicator::connect()"+"\r\n");
		flashP2P.connect(document.connectForm.connectField.value);
		}

   var sendText = function(text)
		{
		flashP2P.send(peer,text);
		}
   
   
   //Callbacks to be called  by FlashP2P
   
   this.onCirrusConnectionStatus = function(ownId, status, statusCode, statusLevel)
   		{
   		dump("FlashCommunicator::onCirrusConnectionStatus() "+ ownId+" "+status+"\r\n");
   		
   		parent.setOwnPeerId(ownId);
   		}		
  
  
   this.onDataArrived = function(peerId, data)
   		{
   		// A message has arrived from a remote peer whose peer id is peerID
	    // This function will decode the message and act accordingly
	   	
	   	dump("FlashCommunicator::onDataArrived(), peerId: "+peerId+" data: "+data+"\r\n")
   
   		
	   	var message = JSON.parse(data);
   		
	   	if (meassage.operation == "request")
	   		{
	   		var listeners = requestListeners;
			for (var i=0; i<listeners.length; i++)
	    		{
	    		listeners[i].onRequest(peerId, message.split_id, message.partition_id);
	    		}
	   		}
	   
	   	if (meassage.operation == "response")
   			{
	   		var listeners = responseListeners;
			for (var i=0; i<listeners.length; i++)
	    		{
	    		listeners[i].onResponse(peerId, message.split_id,message.partition_id,message.data);
	    		}
   			}
   		
	   	if (meassage.operation == "not_found")
   			{
	   		var listeners = errorListeners;
			for (var i=0; i<listeners.length; i++)
	    		{
	    		listeners[i].onError(peerId, data);
	    		}
   			}
   		
   		
   		}	
   		
   this.onConnectionAccepted = function(peerId)
   		{
   		dump("FlashCommunicator::onConnectionAccepted(), connection from peer "+peerId+"\r\n");
   		peer = peerId;
   		flashP2P.addDataListener(peerId,self.onDataArrived);
   		}
   		
   this.onPeerConnectionStatus = function(peerId, status, statusCode, statusLevel)		
   		{
   		dump("FlashCommunicator::onPeerConnectionStatus(), connection to peer "+peerId+" "+status+" "+statusCode+"\r\n");
   		
   		if (status != flashP2P.STATUS_UNKNOWN)
   			document.connectForm.statusField.value = status;
   	
   		if (status == flashP2P.STATUS_CONNECTED)
   			{	
   			peer = peerId;
   			flashP2P.addDataListener(peerId,self.onDataArrived);
   			}
   		}
   		
   
   		
   //initialization	
   
   flashP2P.addCirrusConnectionListener(self.onCirrusConnectionStatus);
   flashP2P.addAcceptListener(self.onConnectionAccepted);
   flashP2P.addPeerConnectionListener(self.onPeerConnectionStatus);
   self.connectToCirrus();
   
   }
