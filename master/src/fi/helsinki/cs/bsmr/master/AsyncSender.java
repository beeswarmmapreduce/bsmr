package fi.helsinki.cs.bsmr.master;

/**
 * The MIT License
 * 
 * Copyright (c) 2010   Department of Computer Science, University of Helsinki
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * Author Sampo Savolainen
 *
 */

import java.io.IOException;

import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.jetty.websocket.WebSocket.Connection;


/**
 * Sends asynchronous messages to outbound WebSocket sockets. There is a thread created for every
 * AsyncSender created. Each thread has a queue of Tasks (a pair of a message String and an outbound
 * socket) they work on. AsyncSenders are addressed via Objects. In other words, each AsyncSender
 * has a "name" (=Object) which is used to identify that sender. For example, each time an AsyncSender
 * is asked for an instance of MasterImpl, the same AsyncSender will be given. The first time an
 * AsyncSender is asked for the MasterImpl object, the AsyncSender will be created along with the thread.
 * 
 * TODO: Should there be a maximum queue length to help detect issues with certain outbound sockets?
 */
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
	
	/**
	 * Get or create an AsyncSender for an object. If no AsyncSender has been created for this object,
	 * one will be created using a default name which includes a string representation of the object.
	 * 
	 * @param o The object for which an AsyncSender is required.
	 * @return The newly created AsyncSender or the one created on the first call for this object
	 * @see AsyncSender#getSender(Object, String)
	 */
	public static AsyncSender getSender(Object o)
	{
		return getSender(o, "AsyncSender for "+o);
	}
	
	/**
	 * Get or create an AsyncSender for an object. If no AsyncSender has been created for this object,
	 * one will be created using the specified title. The title will be used as the title of the thread
	 * created for the new AsyncSender. If an AsyncSender already exists for the object, the title will
	 * have no effect on the thread.
	 *
	 * @param o The object for which an AsyncSender is required.
	 * @param title If this call creates the AsyncSender, this string is used as a title for the AsyncSender thread
	 * @return The newly created AsyncSender or the one created on the first call for this object
	 */
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
	
	/**
	 * Stop the AsyncSender for the given object if a sender has been created.
	 * 
	 * @param o The object for which the AsyncSender has been created for
	 * @return True if there was an AsyncSender for the object to stop
	 */
	public static boolean stopSenderIfPresent(Object o)
	{
		AsyncSender ret;
		
		synchronized (senderForObject) {
			ret = senderForObject.get(o);
		}
		
		if (ret != null) {
			ret.stop();
			return true;
		}
		
		return false;
	}
	
	/**
	 * Stop all AsyncSenders that have been created in this JVM.
	 */
	public static void stopAll()
	{
		synchronized (senderForObject) {
			for (AsyncSender as : senderForObject.values()) {
				logger.info("Stopping AsyncSender "+as.thread.getName());
				as.stop();
			}
		}
	}

	/**
	 * Queue a message for sending. Once the thread starts working on this message, it will
	 * delay for the amount of milliseconds given. This means the delay is cumulative over all the 
	 * queued messages.
	 * 
	 * @param msg The message to send
	 * @param out The socket to send the message to
	 * @param cumulativeDelay Amount of milliseconds to wait after starting to process this message but before sending it 
	 */
	public void sendAsyncMessage(String msg, Connection out, long cumulativeDelay)
	{
		synchronized (messageQueue) {			
			messageQueue.addLast( new Task(msg, out, cumulativeDelay));
			messageQueue.notify();
		}
	}
	
	/**
	 * Queue a message for sending. The message will be sent immediately once the AsyncSender processes this
	 * task.
	 * 
	 * @param msg The message to send
	 * @param out The socket to send the message to
	 */
	public void sendAsyncMessage(String msg, Connection out)
	{
		synchronized (messageQueue) {			
			messageQueue.addLast( new Task(msg, out));
			messageQueue.notify();
		}
	}
	
	
	/**
	 * Stop this AsyncSender. This method clears the current queue, wakes up the Thread
	 * and interrupts it. Note that this method does not however wait for the Thread to
	 * join as the stop() call is needed in cases where this call needs to happen relatively 
	 * fast.
	 */
	public void stop()
	{
		running = false;
		synchronized (messageQueue) {
			messageQueue.clear();
			messageQueue.notify();
		}
		
		thread.interrupt();
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
				Collection<Task> pairsForDeadConnection = new LinkedList<Task>();
				for (Task t : messageQueue) {
					if (t.out == tfe.out) {
						pairsForDeadConnection.add(t);
					}
				}
				messageQueue.removeAll(pairsForDeadConnection);
			}
			
			if (tfe.out != null) {
				tfe.out.disconnect();
			}
			throw new InterruptedException("Stopping AsyncSender");
		}
			
		
	}
	
	private class Task
	{
		String message;
		Connection out;
		long delay;
		
		public Task(String message, Connection out)
		{
			this(message,out,0);
		}
		
		public Task(String message, Connection out, long delay)
		{
			this.message = message;
			this.out = out;
			this.delay = delay;
		}
		
		public void run() throws TaskFailedException
		{
			
			try {
				if (delay > 0) {
					Thread.sleep(delay);
				}
				
				synchronized (out) {
					out.sendMessage(message);
				}
			} catch (IOException ie) {
				throw new TaskFailedException(out, ie);
			} catch (InterruptedException ie) {
				throw new TaskFailedException(ie);
			}
		}
	}
	
	private class TaskFailedException extends Exception
	{
		private static final long serialVersionUID = 1L;
		
		Connection out;
		public TaskFailedException(Connection out, Throwable cause)
		{
			super(cause);
			this.out = out;
		}
		
		public TaskFailedException(Throwable cause)
		{
			super(cause);
			this.out = null;
		}
	}

	/**
	 * Remove all queued tasks that are bound for this socket
	 *  
	 * @param out The socket for which all tasks need to be removed 
	 */
	public void removeTasksFor(Connection out) 
	{
		Set<Task> removeThese = null;
		synchronized (messageQueue) {
			
			for (Task t : messageQueue) {
				if (t.out == out) {
					if (removeThese == null) {
						removeThese = new HashSet<Task>();
					}
					removeThese.add(t);
				}
			}
			
			if (removeThese != null) {
				messageQueue.removeAll(removeThese);
			}
		}
	}

	public boolean hasTasksFor(Connection out) 
	{
		boolean ret = false;
		
		synchronized (messageQueue) {
			for (Task t : messageQueue) {
				if (t.out == out) {
					ret = true;
					break;
				}
			}
		}
		return ret;
	}
}
