package fi.helsinki.cs.bsmr.master.console;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.jetty.util.ajax.JSON;
import org.eclipse.jetty.websocket.WebSocket;

import fi.helsinki.cs.bsmr.master.AsyncSender;
import fi.helsinki.cs.bsmr.master.Job;
import fi.helsinki.cs.bsmr.master.JobAlreadyRunningException;
import fi.helsinki.cs.bsmr.master.MasterContext;
import fi.helsinki.cs.bsmr.master.Message;
import fi.helsinki.cs.bsmr.master.TimeContext;
import fi.helsinki.cs.bsmr.master.Util;

/**
 * Console communication logic.
 * 
 * @author stsavola
 *
 */
public class Console implements WebSocket 
{
	private static Logger logger = Util.getLoggerForClass(Console.class);
	
	private Outbound out;
	private MasterContext master;
	
	
	public Console(MasterContext master)
	{
		this.master = master;
	}
	
	public void disconnect()
	{
		out.disconnect();
	}
	
	@Override
	public void onConnect(Outbound out) 
	{
		TimeContext.markTime();
		logger.fine("connected");
		this.out = out;
		
		// Immediately send out a status message
		sendStatus();
		
		master.addConsole(this);
	}

	@Override
	public void onDisconnect() 
	{
		TimeContext.markTime();
		master.removeConsole(this);
		AsyncSender.stopSenderIfPresent(this);
		
		logger.fine("disconnected");
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onMessage(byte frame, String msg) 
	{
		TimeContext.markTime();
		
		Map<Object, Object> request = (Map<Object, Object>)JSON.parse(msg);
		Object type = request.get("type");
		Map<Object, Object> payload = (Map<Object, Object>)request.get(Message.FIELD_PAYLOAD);
		
		boolean ok = false;
		
		synchronized (master) {
			
			if ("ADDJOB".equals(type)) {
				logger.fine("Console adds a new job");
				if (logger.isLoggable(Level.FINEST)) {
					logger.finest(" ADDJOB, payload: "+payload);
				}
				addJob(payload);
				ok = true;
			} 
			if ("REMOVEJOB".equals(type)) {
				logger.fine("Console removes a job");
				if (logger.isLoggable(Level.FINEST)) {
					logger.finest(" REMOVEJOB, payload: "+payload);
				}
				removeJob(payload);
				ok = true;
			}
			
			if (ok) {
				sendStatus();
			} else {
				logger.info("Unknown message from console: "+msg);
			}
		}
	}

	/**
	 * Handle a REMOVEJOB message from the console.
	 * 
	 * @param payload The payload part of the JSON message
	 */
	private void removeJob(Map<Object, Object> payload)
	{
		int jobId = Util.getIntFromJSONObject(payload.get("id"));
		
		Job toBeRemoved = master.getJobById(jobId);
		if (toBeRemoved == null) {
			logger.warning("Console tried to remove job "+jobId+", but no such job exists");
			return;
		}
		logger.info("Removing job: "+toBeRemoved);
		master.removeJob(toBeRemoved);
	}

	/**
	 * Handle an ADDJOB message from the console.
	 * 
	 * @param payload The payload part of the JSON message
	 */
	private void addJob(Map<Object, Object> payload)
	{
		int splits             = Util.getIntFromJSONObject(payload.get(Message.FIELD_NUM_SPLITS));
		int partitions         = Util.getIntFromJSONObject(payload.get(Message.FIELD_NUM_PARTITIONS));
		int heartbeatTimeout   = Util.getIntFromJSONObject(payload.get("heartbeatTimeout"));
		int acknowledgeTimeout = Util.getIntFromJSONObject(payload.get("progressTimeout"));
		Object code            = payload.get("code");
		
		Job newJob = master.createJob(splits, partitions, heartbeatTimeout, acknowledgeTimeout, code);
		
		logger.info("Adding new job: "+newJob);
		try {
			master.queueJob(newJob);
		} catch(JobAlreadyRunningException jare) {
			logger.log(Level.SEVERE, "This should never happen! A newly created job was already running/had already been ran on the master?!?", jare);
			return;
		}
		
		try {
			master.startNextJob();
		} catch(JobAlreadyRunningException jare) {
			/* NOP, because this just means that there is a job already running */
		}
		
	}

	@Override
	public void onMessage(byte arg0, byte[] arg1, int arg2, int arg3) 
	{
		TimeContext.markTime();
		throw new RuntimeException("onMessage() byte format unsupported!");
	}

	@Override
	public void onFragment(boolean arg0, byte arg1, byte[] arg2, int arg3, int arg4) 
	{
		TimeContext.markTime();
		throw new RuntimeException("onFragment() byte format unsupported!");
	}
	
	/**
	 * Create a new ConsoleInformation message and send it to the console.
	 * 
	 * @see Console#sendMessage(String)
	 */
	public void sendStatus()
	{
		ConsoleInformation ci = new ConsoleInformation(master);
		String msg = ci.toJSONString();
		sendMessage(msg);
	}
	
	/**
	 * Send a message to the console. Sending the message is asynchronous: the actual
	 * transmission is done by a AsyncSender (and thread) dedicated for this console.
	 * 
	 * @param msg The message to send
	 */
	public void sendMessage(String msg)
	{
		AsyncSender sender = AsyncSender.getSender(this);
		sender.sendAsyncMessage(msg, out);
	}
	
}
