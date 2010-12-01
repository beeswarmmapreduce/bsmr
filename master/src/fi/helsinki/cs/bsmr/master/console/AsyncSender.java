package fi.helsinki.cs.bsmr.master.console;

import java.io.IOException;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.jetty.websocket.WebSocket.Outbound;

import fi.helsinki.cs.bsmr.master.Util;


public class AsyncSender implements Runnable 
{
	private static Logger logger = Util.getLoggerForClass(AsyncSender.class);
	
	private static Map<Object, AsyncSender> senderForObject = new HashMap<Object, AsyncSender>();
	
	private Deque<Task> messageQueue;
	private Object owner;
	private boolean running;
	
	private Thread thread;
	
	private AsyncSender(Object owner)
	{
		this.messageQueue = new LinkedList<Task>();
		this.owner = owner;
	}
	
	public static AsyncSender getSender(Object o)
	{
		return getSender(o, "AsyncSender for "+o);
	}
	
	public static AsyncSender getSender(Object o, String title)
	{
		AsyncSender ret;
		
		synchronized (senderForObject) {
			ret = senderForObject.get(o);
			
			if (ret == null) {
				ret = new AsyncSender(o);
				senderForObject.put(o, ret);
				
				ret.thread = new Thread(ret, title);
				ret.thread.start();
			}
		}
		
		return ret;
	}
	
	public static void stopSenderIfPresent(Object o)
	{
		AsyncSender ret;
		
		synchronized (senderForObject) {
			ret = senderForObject.get(o);
		}
		
		if (ret != null) {
			ret.stop();
		}
		
	}
	
	public static void stopAll()
	{
		synchronized (senderForObject) {
			for (AsyncSender as : senderForObject.values()) {
				logger.info("Stopping AsyncSender "+as.thread.getName());
				as.stop();
			}
		}
	}
	
	public void sendAsyncMessage(String msg, Outbound out)
	{
		// TODO: Add queue max length where too many queued messages would kill this sender!
		synchronized (messageQueue) {			
			messageQueue.addLast( new Task(msg, out));
			messageQueue.notify();
		}
	}
	
	
	public void stop()
	{
		running = false;
		synchronized (messageQueue) {
			messageQueue.clear();
			messageQueue.notify();
		}
		
		thread.interrupt();
		// NOTE: no thread.join() because this might be called in situations where this needs to exit relatively fast
	}
	
	@Override
	public void run() 
	{
		running = true;
		try {
		
			while(running) {
				runOnce();
			}
			
		} catch(InterruptedException ie) {
			logger.log(Level.WARNING,"I was interrupted! Exiting..", ie);
		} catch(NoSuchElementException nsee) {
			// This is triggered (almost always) when stop() is called
			
		} finally {
			synchronized (senderForObject) {
				// And remove this AsyncSender from the map
				senderForObject.remove(owner);
			}
		}
		
		
	}

	private void runOnce() throws InterruptedException, NoSuchElementException
	{
		Task nextTask;
		
		synchronized (messageQueue) {
			if (messageQueue.isEmpty()) {
				messageQueue.wait();
			}
		
			nextTask = messageQueue.pop();
		}
		
		// Stop if we were notified to stop
		if (nextTask == null) {
			return;
		}
		
		try {
			nextTask.run();
		} catch(TaskFailedException tfe) {
			logger.log(Level.SEVERE, "Asynchronous send failed, disconnecting endpoint "+owner+" and flushing further messages", tfe);
			
			synchronized (messageQueue) {
				Collection<Task> pairsForDeadOutbound = new LinkedList<Task>();
				for (Task t : messageQueue) {
					if (t.out == tfe.out) {
						pairsForDeadOutbound.add(t);
					}
				}
				messageQueue.removeAll(pairsForDeadOutbound);
			}
			
			tfe.out.disconnect();
			throw new InterruptedException("Stopping AsyncSender");
		}
			
		
	}
	
	private class Task
	{
		String message;
		Outbound out;
		
		public Task(String message, Outbound out)
		{
			this.message = message;
			this.out = out;
		}
		
		public void run() throws TaskFailedException
		{
			
			try {
				out.sendMessage(message);
			} catch (IOException ie) {
				throw new TaskFailedException(out, ie);
			}
		}
	}
	
	private class TaskFailedException extends Exception
	{
		private static final long serialVersionUID = 1L;
		
		Outbound out;
		public TaskFailedException(Outbound out, Throwable cause)
		{
			super(cause);
			this.out = out;
		}
	}
}
