//The messages that are used for p2p reques-response communication (over flashp2p, at least)

function RequestMessage(_peerId, _splitId, _partitionId)
{
var self = this;

this.getPeerId = function()
	{
	return _peerId;
	}

this.getSplitId = function() 
	{
	return _splitId;
	}

this.getPartitionId = function() 
	{
	return _partitionId;
	}

this.serialize = function()
	{
	return JSON.stringify( {operation: "request", splitId: _splitId, partitionId: _partitionId } );
	}
}

function ResponseMessage(_peerId, _splitId, _partitionId, _data)
{
var self = this;

this.getPeerId = function()
	{
	return _peerId;
	}

this.getSplitId = function() 
	{
	return _splitId;
	}

this.getPartitionId = function() 
	{
	return _partitionId;
	}

this.getData = function() 
	{
	return _data;
	}

this.serialize = function()
	{
	return JSON.stringify( {operation: "response", splitId: _splitId, partitionId: _partitionId, data: _data } );
	}	


}