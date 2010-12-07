package fi.helsinki.cs.bsmr.master;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;


import org.eclipse.jetty.websocket.WebSocket;

import fi.helsinki.cs.bsmr.master.Message.Type;
import fi.helsinki.cs.bsmr.master.console.AsyncSender;

public class Worker implements WebSocket 
{
	private static Logger logger = Util.getLoggerForClass(Worker.class);
	
	private Outbound out;
	private MasterContext master;
	private String workerRemoteAddr;
	
	private long lastHearbeat;
	private long lastProgress;
	
	private long connectTime;

	
	Worker(MasterContext workerStore, String remoteAddr)
	{
		this.master = workerStore;
		this.workerRemoteAddr = remoteAddr;
		
		// Before worker starts communicating, it should be "dead":
		this.lastHearbeat = -1;
		this.lastProgress = -1;
		
		this.connectTime = TimeContext.now();
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

		this.lastHearbeat = this.lastProgress = TimeContext.now();
		
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
		TimeContext.markTime();
		
		// Make sure nobody relies on this worker anymore
		lastHearbeat = -1;
		lastProgress = -1;
		
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
		TimeContext.markTime();
		
		if (logger.isLoggable(Level.FINE)) {
			logger.finest( "onMessage(): '"+jsonMsg+"' (frame "+frame+")");
		}	
		
		Message msg;
		
		try {
			msg = Message.parseMessage(jsonMsg, master, workerRemoteAddr);
		} catch(IllegalMessageException ime) {
			logger.log(Level.SEVERE,"Illegal message from worker", ime);
			if (logger.isLoggable(Level.FINE)) {
				logger.fine("illegal message contents: '"+ime.getProblematicMessage()+"'");
			}
			// there could be an error message to inform the worker
			return;
		}
		
		if (msg.getType() == Type.DO) {
			logger.severe("Workers cannot send DO messages: "+msg);
			return;
		}
		
		String reply;

		if (msg.getAction() == Message.Action.socket) {
			master.setWorkerURL(this, msg.getSocketURL());
		}
	
		if (!master.isJobActive()) {
			
			if (msg.getType() == Type.HB) {
				logger.warning("A worker sent a non-heartbeat while no active job");
			}
			
			lastHearbeat = TimeContext.now();
			lastProgress = TimeContext.now();
			
			reply = null;
			
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
			
			
			// If these two tasks are not grouped together, bad things will happen..
			synchronized (master) {
				
				boolean moreToBeDone = master.acknowledgeWork(this, msg);
				
				if (moreToBeDone) {
					Message tmp = master.selectTaskForWorker(this, msg);
					reply = tmp.encodeMessage();
				} else {
					reply = null;
				}

			}

		}
	
		
		if (reply != null) {
			try {
				sendSyncMessage(reply);
			} catch(IOException ie) {
				logger.log(Level.SEVERE, "Could not reply to worker. Terminating connection.", ie);			
				disconnect();
			}
			
		}

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

	public void sendAsyncMessage(Message reply) throws IOException
	{
		AsyncSender sender = AsyncSender.getSender(master);		
		sender.sendAsyncMessage(reply.encodeMessage(), out);
	}
	
	public void sendAsyncMessage(Message reply, long cumulativeDelay) throws IOException
	{
		AsyncSender sender = AsyncSender.getSender(master);		
		sender.sendAsyncMessage(reply.encodeMessage(), out, cumulativeDelay);
	}
	
	public void sendSyncMessage(String reply) throws IOException
	{
		synchronized (out) { // See synchronization at AsyncSender$Task.run()
			
			// TODO: We could remove any message to this "out" from the AsyncSender
			out.sendMessage(reply);
		}
	}


	public String getSocketURL() 
	{
		return master.getWorkerURL(this);
	}

	public long getConnectTime()
	{
		return connectTime;
	}
	
	@Override
	public void onMessage(byte arg0, byte[] arg1, int arg2, int arg3)
	{
		TimeContext.markTime();
		logger.severe("onMessage() byte format unsupported!");
		throw new RuntimeException("onMessage() byte format unsupported!");
	}

	@Override
	public void onFragment(boolean arg0, byte arg1, byte[] arg2, int arg3,
			int arg4) 
	{
		TimeContext.markTime();
		logger.severe("onFragment() byte format unsupported!");
		throw new RuntimeException("onFragment() byte format unsupported!");
	}
	
}
