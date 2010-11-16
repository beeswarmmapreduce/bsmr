package fi.helsinki.cs.bsmr.master;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import org.eclipse.jetty.websocket.WebSocket;


public class Master
{
	private static Logger logger = Util.getLoggerForClass(Master.class);
	
	private Set<Worker> workers;
	
	private Object executeLock; 
	
	private List<Job> jobQueue;
	private Job activeJob;
	
	public Master()
	{
		executeLock = new Object();
		workers     = new HashSet<Worker>();
		activeJob   = null;
		jobQueue    = new LinkedList<Job>();
	}

	public Worker createWorker()
	{
		return new Worker(this);
	}
	
	public WebSocket createConsole()
	{	
		return new Console(this);
	}

	public void invalidateWorker(Worker worker, Throwable t) 
	{
		synchronized (executeLock) 
		{
			removeWorker(worker);
		}
		
		worker.disconnect();
	}

	
	
	public Message executeWorkerMessage(Worker worker, Message msg)
	{
		if (msg.getType() == Message.Type.DO) {
			logger.severe("Workers can't send DO messages!");
			return null; // TODO
		}
		
		synchronized (executeLock) 
		{
			if (msg.getAction() == Message.Action.UP) {
				
			}
			// Do whatever
			
			// Check if everything is mapped
		}
	
		return new Message(Message.Type.DO, Message.Action.MAP, 1);
	}


	public void removeWorker(Worker worker) 
	{
		boolean removed;
		
		synchronized (executeLock) 
		{			
			removed = workers.remove(worker); 
		}
		
		if (!removed) {
			throw new RuntimeException("removeWorker() worker "+worker+" was not registered");
		}
	}
	
	public void addWorker(Worker worker)
	{
		synchronized (executeLock) 
		{
			if (workers.contains(worker)) {
				throw new RuntimeException("addWorker() worker "+worker+" already exists");
			}
			
			workers.add(worker);
		}
		
	}
	
	public Job getActiveJob()
	{
		return activeJob;
	}

}
