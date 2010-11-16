package fi.helsinki.cs.bsmr.master;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.jetty.websocket.WebSocket;

import fi.helsinki.cs.bsmr.master.Message.Action;
import fi.helsinki.cs.bsmr.master.Message.Type;

public class Worker implements WebSocket 
{
	private static Logger logger = Util.getLoggerForClass(Worker.class);
	
	private Outbound out;
	private Master master;
	
	private long lastHearbeat;
	private long lastAcknowledge;
	
	
	Worker(Master master)
	{
		this.master = master;
		
		// Before worker starts communicating, it should be "dead":
		this.lastHearbeat = -1;
		this.lastAcknowledge = -1;
	}
	

	public void disconnect() 
	{
		out.disconnect();
	}

	/** WebSocket callback implementations **/
	
	@Override
	public void onConnect(Outbound out) 
	{
		logger.fine("onConnect()");
		this.out = out;

		// TODO: is this best here, or should we wait for the UP message?
		this.lastHearbeat = this.lastAcknowledge = System.currentTimeMillis();
		
		master.addWorker(this);
	}

	@Override
	public void onDisconnect()
	{
		logger.fine("onDisconnect()");
		
		master.removeWorker(this);
	}


	@Override
	public void onMessage(byte frame, String jsonMsg) 
	{
		if (logger.isLoggable(Level.FINE)) {
			logger.fine( "onMessage(): '"+jsonMsg+"' (frame "+frame+")");
		}	
		
		Message msg = Message.parseMessage(jsonMsg);
		
		if (msg.getType() != Type.ACK) {
			logger.severe("unsupported message type to worker: "+msg);

			return;
		}
		
		if (msg.getAction() == Action.HEARTBEAT) {
			lastHearbeat = System.currentTimeMillis();
			return;
		}
		
		Message reply = master.executeWorkerMessage(this, msg);
		
	
		
		try {
			out.sendMessage(reply.encodeMessage());
		} catch(IOException ie) {
			master.invalidateWorker(this, ie);
		}

	}

	@Override
	public void onMessage(byte arg0, byte[] arg1, int arg2, int arg3)
	{
		logger.severe("onMessage() byte format unsupported!");
		
		throw new RuntimeException("onMessage() byte format unsupported!");
	}


	public boolean isAvailable(Job job)
	{
		if (isDead(job)) return false;
		return ( (System.currentTimeMillis() - lastHearbeat)    < job.getWorkerHeartbeatTimeout());
	}
	

	public boolean isDead(Job job)
	{
		return ( (System.currentTimeMillis() - lastAcknowledge) > job.getWorkerAcknowledgeTimeout());
	}

/** The relationship between Job and the availability of the Worker is more clear in the upper versions
	public boolean isAvailable()
	{
		if (isDead()) return false;
		return ( (System.currentTimeMillis() - lastHearbeat)    < master.getActiveJob().getWorkerHeartbeatTimeout());
	}
	
	public boolean isDead()
	{
		return ( (System.currentTimeMillis() - lastAcknowledge) > master.getActiveJob().getWorkerAcknowledgeTimeout());
	}
**/
}
