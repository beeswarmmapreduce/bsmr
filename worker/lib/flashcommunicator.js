// The class that implements a simple request-response protocol for exchanging data using
// FlashP2P


function FlashCommunicator()
   {
   var self = this;
   var connectedPeers = new Object();
   var outgoingQueue = new Array();			//if no connection to a peer, the message gets queued here 
   
   var responseListeners = new Array();
   
   
   //Public interface towards BSMR's FlashInter
   
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
  
   this.addResponseListener = function(listener)
   		{
	   	responseListeners.push(listener);
   		}
   
   
   //Private functions  implementing the public interface
   
   
   
   
   this.connectToCirrus = function()
   		{
   		flashP2P.connectToCirrus("rtmfp://p2p.rtmfp.net/ac5e767303706ffbc314d1e3-7b419bc16675/");
   		}
   		
   this.onCirrusConnectionStatus = function(ownId, status, statusCode, statusLevel)
   		{
   		dump("Application::onCirrusConnectionStatus() "+ ownId+" "+status+"\r\n");
   		
   		if (status != flashP2P.STATUS_UNKNOWN)
   			document.testForm.statusField.value = status;
   		
   		document.testForm.idField.value = ownId; 
   		}		
   
   this.listen = function()
   		{
   		dump("Application::listen()\r\n");
   		flashP2P.listen();
   		}
   
   this.onDataArrived = function(peerId, data)
   		{
   		dump("Application::onDataArrived(), peerId: "+peerId+" data: "+data+"\r\n")
   		document.htmlForm.receivedField.value = data;
   		}	
   		
   this.onConnectionAccepted = function(peerId)
   		{
   		dump("Application::onConnectionAccepted(), connection from peer "+peerId+"\r\n");
   		peer = peerId;
   		flashP2P.addDataListener(peerId,self.onDataArrived);
   		}
   		
   this.onPeerConnectionStatus = function(peerId, status, statusCode, statusLevel)		
   		{
   		dump("Application::onPeerCOnnectionStatus(), connection to peer "+peerId+" "+status+" "+statusCode+"\r\n");
   		
   		if (status != flashP2P.STATUS_UNKNOWN)
   			document.connectForm.statusField.value = status;
   	
   		if (status == flashP2P.STATUS_CONNECTED)
   			{	
   			peer = peerId;
   			flashP2P.addDataListener(peerId,self.onDataArrived);
   			}
   		}
   		
   this.connect = function()
   		{
   		dump("Application::connect()"+"\r\n");
   		flashP2P.connect(document.connectForm.connectField.value);
   		}
   
   this.sendText = function(text)
   		{
   		flashP2P.send(peer,text);
   		}
   		
   	
   
   flashP2P.addCirrusConnectionListener(self.onCirrusConnectionStatus);
   flashP2P.addAcceptListener(self.onConnectionAccepted);
   flashP2P.addPeerConnectionListener(self.onPeerConnectionStatus);
   
   }
