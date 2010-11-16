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
	private long lastProgress;
	
	
	Worker(Master master)
	{
		this.master = master;
		
		// Before worker starts communicating, it should be "dead":
		this.lastHearbeat = -1;
		this.lastProgress = -1;
	}
	

	public void disconnect() 
	{
		// Make sure nobody relies on this worker anymore
		lastHearbeat = -1;
		lastProgress = -1;
		
		out.disconnect();
	}

	/** WebSocket callback implementations **/
	
	@Override
	public void onConnect(Outbound out) 
	{
		logger.fine("onConnect()");
		this.out = out;

		// TODO: is this best here, or should we wait for the UP message?
		this.lastHearbeat = this.lastProgress = System.currentTimeMillis();
		
		try {
			master.addWorker(this);
		} catch(WorkerInIllegalStateException wiise) {
			logger.log(Level.SEVERE, "New worker already registered?!?", wiise);
			disconnect();
		}
	}

	@Override
	public void onDisconnect()
	{
		logger.fine("onDisconnect()");
		
		try { 
			master.removeWorker(this);
		} catch(WorkerInIllegalStateException wiise) {
			logger.log(Level.SEVERE, "Disconnected worker not registered?!?", wiise);
		}
		
	}


	/**
	 * Division of labor between Worker.onMessage() and Master.executeWorkerMessage() is 
	 * still a bit unclear. Current attempt is: worker level functionality (e.g. heart beat)
	 * while master provides access to the current work. However making sure the worker
	 * uses the correct job id needs to be in Worker.onMessage() as one needs to react
	 * before parsing the heart beat.
	 * 
	 * Note that locking which usually starts in the master is started here to avoid
	 * concurrency issues with switching jobs. 
	 * 
	 * 
	 */
	@Override
	public void onMessage(byte frame, String jsonMsg) 
	{
		if (logger.isLoggable(Level.FINE)) {
			logger.finest( "onMessage(): '"+jsonMsg+"' (frame "+frame+")");
		}	
		
		Message msg = Message.parseMessage(jsonMsg);
		
		if (msg.getType() != Type.ACK) {
			logger.severe("unsupported message type to worker: "+msg);
			return;
		}
		
		Message reply;
		
		synchronized (master.executeLock) {

			// TODO: !jobID => reset
			
			if (msg.getJob() != master.getActiveJob()) {
				// TODO: => reset
			}
			
			if (msg.getAction() == Action.heartbeat) {
				logger.fine("heartbeat");
				lastHearbeat = System.currentTimeMillis();
				return; // there is no reply to heart beat messages
			}
			
			// This could be moved to Master, but this should be enough for a prototype
			lastProgress = lastHearbeat;
			
			
			// TODO: parse a "where is worker message"?
			
			reply = master.executeWorkerMessage(this, msg);
			
		}
		
		try {
			out.sendMessage(reply.encodeMessage());
		} catch(IOException ie) {
			logger.log(Level.SEVERE, "Could not reply to worker. Terminating connection.", ie);
			
			try {
				master.removeWorker(this);
			} catch(WorkerInIllegalStateException wiise) {
				logger.log(Level.SEVERE, "The worker we could not send a reply to was not registered as a worker?", wiise);
			} finally {
				disconnect();
			}
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
		return ( (System.currentTimeMillis() - lastProgress) > job.getWorkerAcknowledgeTimeout());
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
