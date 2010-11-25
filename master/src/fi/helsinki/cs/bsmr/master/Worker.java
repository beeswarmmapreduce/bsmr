package fi.helsinki.cs.bsmr.master;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;


import org.eclipse.jetty.websocket.WebSocket;

import fi.helsinki.cs.bsmr.master.Message.Type;

public class Worker implements WebSocket 
{
	private static Logger logger = Util.getLoggerForClass(Worker.class);
	
	private Outbound out;
	private WorkerStore workerStore;
	private String workerRemoteAddr;
	
	private long lastHearbeat;
	private long lastProgress;

	
	Worker(WorkerStore workerStore, String remoteAddr)
	{
		this.workerStore = workerStore;
		this.workerRemoteAddr = remoteAddr;
		
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
		TimeContext.markTime();
		
		logger.fine("onConnect()");
		this.out = out;

		// TODO: is this best here, or should we wait for the UP message?
		this.lastHearbeat = this.lastProgress = TimeContext.now();
		
		try {
			workerStore.addWorker(this);
		} catch(WorkerInIllegalStateException wiise) {
			logger.log(Level.SEVERE, "New worker already registered?!?", wiise);
			disconnect();
		}
	}

	@Override
	public void onDisconnect()
	{
		TimeContext.markTime();
		
		// Make sure nobody relies on this worker anymore
		lastHearbeat = -1;
		lastProgress = -1;
		
		logger.fine("onDisconnect()");
		
		try { 
			workerStore.removeWorker(this);
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
		TimeContext.markTime();
		
		if (logger.isLoggable(Level.FINE)) {
			logger.finest( "onMessage(): '"+jsonMsg+"' (frame "+frame+")");
		}	
		
		Message msg;
		
		try {
			msg = Message.parseMessage(jsonMsg, workerStore, workerRemoteAddr);
		} catch(IllegalMessageException ime) {
			logger.log(Level.SEVERE,"Illegal message from worker", ime);
			if (logger.isLoggable(Level.FINE)) {
				logger.fine("illegal message contents: '"+ime.getProblematicMessage()+"'");
			}
			return; // TODO: should the worker be informed? .. naah
		}
		
		if (msg.getType() == Type.DO) {
			logger.severe("Workers cannot send DO messages: "+msg);
			return;
		}
		
		Message reply;
		
		synchronized (workerStore) {

			if (msg.getAction() == Message.Action.socket) {
				workerStore.setWorkerURL(this, msg.getSocketURL());
			}
		
				
			if (!workerStore.isActive()) {
				
				if (msg.getType() == Type.HB) {
					logger.warning("A worker sent a non-heartbeat while no active job");
					// TODO: send new idle?
					reply = null; // set to idle message
				} else {
					reply = null;
				}
				
				lastHearbeat = TimeContext.now();
				lastProgress = TimeContext.now();
				
				
				
			} else {
				
				// The heart beat is always updated
				lastHearbeat = TimeContext.now();
				
				if (msg.getType() == Type.HB) {
					// If just a pure heart beat, we do not reply
					if (msg.getAction() != Message.Action.idle) {
						logger.fine("heartbeat");
						return;
					}
					logger.info("Worker idle but there's work to do");
				}

				lastProgress = TimeContext.now();
				
				
				boolean moreToBeDone = workerStore.acknowledgeWork(this, msg);
				
				if (moreToBeDone) {
					reply = workerStore.selectTaskForWorker(this, msg);
				} else {
					reply = null;
				}
				
				
			}
		}
		
		if (reply != null) {
			try {
				sendMessage(reply);
			} catch(IOException ie) {
				logger.log(Level.SEVERE, "Could not reply to worker. Terminating connection.", ie);
				
				try {
					workerStore.removeWorker(this);
				} catch(WorkerInIllegalStateException wiise) {
					logger.log(Level.SEVERE, "The worker we could not send a reply to was not registered as a worker?", wiise);
				} finally {
					disconnect();
				}
			}
			
		}

	}

	@Override
	public void onMessage(byte arg0, byte[] arg1, int arg2, int arg3)
	{
		TimeContext.markTime();
		logger.severe("onMessage() byte format unsupported!");
		throw new RuntimeException("onMessage() byte format unsupported!");
	}


	public boolean isAvailable(Job job)
	{
		if (isDead(job)) return false;
		return ( (TimeContext.now() - lastHearbeat)    < job.getWorkerHeartbeatTimeout());
	}
	

	public boolean isDead(Job job)
	{
		return ( (TimeContext.now() - lastProgress) > job.getWorkerAcknowledgeTimeout());
	}

	public void sendMessage(Message reply) throws IOException
	{
		out.sendMessage(reply.encodeMessage());		
	}


	public String getSocketURL() 
	{
		return workerStore.getWorkerURL(this);
	}
	
}
