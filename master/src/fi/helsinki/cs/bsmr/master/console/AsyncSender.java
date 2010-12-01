package fi.helsinki.cs.bsmr.master.console;

import java.io.IOException;
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
	
	private Deque<String> messageQueue;
	private Outbound out;
	private Object owner;
	private boolean running;
	
	private AsyncSender(Object owner, Outbound out)
	{
		this.messageQueue = new LinkedList<String>();
		this.out = out;
		this.owner = owner;
	}
	
	public static AsyncSender getSender(Object o, Outbound out)
	{
		AsyncSender ret;
		
		synchronized (senderForObject) {
			ret = senderForObject.get(o);
			
			if (ret == null) {
				ret = new AsyncSender(o, out);
				senderForObject.put(o, ret);
				
				Thread thr = new Thread(ret, "AsyncSender for "+o);
				thr.start();
			}
		}
		
		if (ret.out != out) {
			logger.severe("Sender for object "+o+" requested with a different outbound connection!?! Using the original connection instead");
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
	
	public void sendAsyncMessage(String msg)
	{
		// TODO: Add queue max length where too many queued messages would kill this sender!
		synchronized (messageQueue) {			
			messageQueue.addLast(msg);
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
		String nextMessage;
		synchronized (messageQueue) {
			if (messageQueue.isEmpty()) {
				messageQueue.wait();
			}
		
			nextMessage = messageQueue.pop();
		}
		
		try {
			out.sendMessage(nextMessage);
		} catch (IOException ie) {
			logger.log(Level.SEVERE, "Asynchronous send failed, disconnecting endpoint "+owner, ie);
			out.disconnect();
			throw new InterruptedException("Stopping AsyncSender");
		}
	}
}
