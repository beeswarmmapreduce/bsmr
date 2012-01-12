//This is a JavaScript wrapper for the P2P functionality of Adobe Flash

addFlashBinary();

function FlashP2P()
    {
    console.log("FlashP2P()");
    
    var self = this;
    
    this.STATUS_ERROR = "STATUS_ERROR";
    this.STATUS_UNKNOWN = "STATUS_UNKNOWN";
    this.STATUS_CONNECTED = "STATUS_CONNECTED";
    this.STATUS_CLOSED = "STATUS_CLOSED";
    
    var cirrusConnectionListeners = new Array();
    var peerConnectionListeners = new Array();
    var acceptListeners = new Array();
    var dataListeners = new Object();
    
    var ownId = "";
    
    
    // Interface towards Javascript
    
   this.listen = function()
		{
		console.log("FlashP2P::listen()\r\n");
		getFlashMovie("JavascriptInterface").listen();
		}	 	 
   
   this.connect = function(peerId)
    	{
    	console.log("FlashP2P::connect()\r\n");
    	getFlashMovie("JavascriptInterface").connect(peerId);
    	}
   
    this.send = function(peerId, data)
    	{
    	getFlashMovie("JavascriptInterface").send(peerId,data);
    	}
    	
    this.addCirrusConnectionListener = function(listener)
    	{
    	cirrusConnectionListeners.push(listener);
    	}
    	
    	
    this.addPeerConnectionListener = function(listener)
    	{
    	peerConnectionListeners.push(listener);
    	}
    
    this.addAcceptListener = function(listener)
    	{
    	acceptListeners.push(listener);
    	}
    
    this.addDataListener = function(peerId,listener)
    	{
    	if (!dataListeners[peerId])
    		dataListeners[peerId] = new Array();
    	
    	dataListeners[peerId].push(listener);  
    	}
    
    this.connectToCirrus = function(cirrusUrl)
    	{
    	var foo = getFlashMovie("JavascriptInterface");
    	console.log(foo);
    	
    	getFlashMovie("JavascriptInterface").connectToCirrus(cirrusUrl);
    	}
    	
    
    
    //Interface towards Flash	
    
    this.onDataArrived = function(peerId, data)
		{
		console.log("FlashP2P::onDataArrived(), peerId: "+peerId+" data: "+data+"\r\n");
		var listeners = dataListeners[peerId];
		for (var i=0; i<listeners.length; i++)
    		{
    		listeners[i](peerId, data);
    		}
		}
		
    this.onCirrusConnectionStatus = function(nearId, statusCode, statusLevel)
    	{
    	console.log("FlashP2P::onCirrusConnectionStatus(), ownId: "+nearId+"\r\n");
    	ownId = nearId;
    	
    	var status = self.STATUS_UNKNOWN;
    	   
    	if (statusLevel == "error")
    		status = self.STATUS_ERROR;
    		
    	if (statusCode == "NetConnection.Connect.Success")
    		status = self.STATUS_CONNECTED;
    		
    	if (statusCode == "NetConnection.Connect.Closed")
    		status = self.STATUS_CLOSED;
    		
    	for (var i=0; i<cirrusConnectionListeners.length; i++)
    		{
    		cirrusConnectionListeners[i](nearId, status, statusCode, statusLevel);
    		}
    	}
    		
    this.onConnectionAccepted = function(peerId)
    	{
    	console.log("FlashP2P::onConnectionAccepted(), peerId: "+peerId+"\r\n");
    	for (var i=0; i<acceptListeners.length; i++)
    		{
    		acceptListeners[i](peerId);
    		}
    	}	
    	
	this.onPeerConnectionStatus = function(peerId, statusCode, statusLevel)
     	{
     	console.log("FlashP2P::onPeerConnectionStatus(), peerId: "+peerId+"\r\n");
     	
     	var status = self.STATUS_UNKNOWN;
     	
     	if (statusLevel == "error")
    		status = self.STATUS_ERROR;	
    	
    	if (statusCode == "NetStream.Play.Reset")
    		status = self.STATUS_CONNECTED;
     	
     	if (statusCode == "NetStream.Connect.Closed")
    		status = self.STATUS_CLOSED;
     	
     	for (var i=0; i<peerConnectionListeners.length; i++)
    		{
    		peerConnectionListeners[i](peerId, status, statusCode, statusLevel);
    		}
     	}
				
    }