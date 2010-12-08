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

	
	/**
	 * Create a new worker for the master in question.
	 * 
	 * @param masterContext The master for which the worker is created for
	 * @param remoteAddr The remote address (name or IP in decimal-dot format) of the worker
	 */
	Worker(MasterContext masterContext, String remoteAddr)
	{
		this.master = masterContext;
		this.workerRemoteAddr = remoteAddr;
		
		// Before worker starts communicating, it should be "dead":
		this.lastHearbeat = -1;
		this.lastProgress = -1;
		
		this.connectTime = TimeContext.now();
	}
	

	/**
	 * Force this worker to disconnect
	 */
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
	 * Parse the incoming message and handle it. There is a division of labor between this
	 * function and the master. The following steps are parsed here: socket announcements, 
	 * updating heart beat and acknowledgment timers. If the message contains data to be
	 * acknowledged and/or new work should be allocated for the worker, the code calls the
	 * master to do these tasks. These tasks are grouped into a synchronized block to handle
	 * concurrency issues that would arise with multiple workers messaging at the same time.
	 * 
	 * @see MasterContext#acknowledgeWork(Worker, Message)
	 * @see MasterContext#selectTaskForWorker(Worker, Message)
	 */
	@Override
	public void onMessage(byte frame, String jsonMsg) 
	{
		TimeContext.markTime();
		
		if (logger.isLoggable(Level.FINE)) {
			logger.finest( "onMessage(): '"+jsonMsg+"' (frame "+frame+")");
		}	
		
		Message msg;
		String reply;
		
		try {
			// WARN: this might not be thread safe
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
		
		

		if (msg.getAction() == Message.Action.socket) {
			master.setWorkerURL(this, msg.getSocketURL());
		}
	
		if (msg.getJob() != null && msg.getJob() != master.getActiveJob()) {
			logger.warning("Worker used a non-active job");
		}
		
		
		if (!master.isJobRunning()) {
			
			if (msg.getType() == Type.ACK && msg.getAction() != Message.Action.socket) {
				logger.warning("A worker sent a non-heartbeat, non-socket while no active job");
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
				logger.fine("Worker idle but there's work to do. Let's give it some!");
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

	/**
	 * Whether this worker has sent a heart beat message within the heart beat timeout specified
	 * for this job. If the worker is dead, then it will not be available regardless when the
	 * last heart beat message has been sent. Note that the worker heart beat timer is updated for all
	 * incoming messages.
	 * 
	 * @param job The job whose heart beat timeout value is used
	 * @return True if the worker is not dead and if heart beat timeout has not elapsed since the previous
	 *         worker activity.  
	 * @see Worker#isDead(Job)
	 */
	public boolean isAvailable(Job job)
	{
		if (isDead(job)) return false;
		return ( (TimeContext.now() - lastHearbeat)    < job.getWorkerHeartbeatTimeout());
	}
	
	/**
	 * Whether this worker has acknowledged work within the acknowledgment timeout specified for
	 * this job. Note that the acknowledgment timer is updated only for messages acknowledging work.
	 * If there is no active job for the master this worker has been created for, heart beat messages will
	 * also update the acknowledgment timer.
	 *  
	 * @param job The job whose acknowledgment timeout is used
	 * @return True if the worker has acknowledged work before the acknowledgment timeout has passed.
	 */
	public boolean isDead(Job job)
	{
		return ( (TimeContext.now() - lastProgress) > job.getWorkerAcknowledgeTimeout());
	}

	/**
	 * Send an asynchronous message to the worker. Asynchronous messages for workers are handled by a
	 * separate thread sending the messages via a FIFO. Note that the message is encoded within this call.
	 * Encoding messages needs to be done within the "big lock".
	 *  
	 * @param msg The message to be sent
	 */
	public void sendAsyncMessage(Message msg) 
	{
		AsyncSender sender = AsyncSender.getSender(master);		
		sender.sendAsyncMessage(msg.encodeMessage(), out);
	}
	
	/**
	 * Send an asynchronous message to the worker. Asynchronous messages for workers are handled by a
	 * separate thread sending the messages via a FIFO. Note that the message is encoded within this call.
	 * Encoding messages needs to be done within the "big lock".
	 *  
	 * @param msg The message to be sent
	 * @param cumulativeDelay When the sending thread processes this message, it will wait this many milliseconds
	 *                        before sending out this message. This is only used for a ramp up time when starting
	 *                        a new job.
	 */
	public void sendAsyncMessage(Message msg, long cumulativeDelay)
	{
		AsyncSender sender = AsyncSender.getSender(master);		
		sender.sendAsyncMessage(msg.encodeMessage(), out, cumulativeDelay);
	}
	
	/**
	 * Send the message synchronously in the current thread. The message is as a String so that the message encoding
	 * can be done before sending the message. This eliminates the need to make this call within the "big lock".
	 * Sending is synchronized on the Outbound object used by this worker. This is to eliminate concurrency issues
	 * with the async and sync messaging. See AsyncSender$Task.run().
	 *  
	 * @param msg The message to the worker
	 * @throws IOException If the synchronous send fails for some reason.
	 */
	public void sendSyncMessage(String msg) throws IOException
	{
		
		synchronized (out) { // See synchronization at AsyncSender$Task.run()
			
			// Remove all async messages bound for this connection as this latest message
			// needs to override any previous ones.
			
			AsyncSender sender = AsyncSender.getSender(master);
			sender.removeTasksFor(out);
				
			out.sendMessage(msg);
		}
	}

	/**
	 * Get the Socket URL for this worker. Note that the URL is actually stored in the master for which the worker
	 * is created for.
	 *  
	 * @return The URL for this worker.
	 */
	public String getSocketURL() 
	{
		return master.getWorkerURL(this);
	}

	/**
	 * Get the time at when this worker connected.
	 * 
	 * @return Connect time in milliseconds.
	 */
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
