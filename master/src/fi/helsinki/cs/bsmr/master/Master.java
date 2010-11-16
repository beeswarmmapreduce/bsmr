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

	public void queueJob(Job j) throws JobAlreadyRunningException
	{
		if (j.isStarted() || j.isFinished()) {
			logger.severe("Tried to add job "+j+" to job queue, but it's in an illegal state (started or finished)");
			throw new JobAlreadyRunningException("Tried to add job "+j+" to job queue, but it's in an illegal state (started or finished)");
		}
		
		jobQueue.add(j);
	}
	
	public boolean startNextJob() throws JobAlreadyRunningException
	{
		if (activeJob != null && !activeJob.isFinished()) {
			logger.severe("Tried to start next job, but we have a non-finished active job!");
			throw new JobAlreadyRunningException("Tried to start next job, but we have a non-finished active job!");
		}
		
		if (jobQueue.isEmpty()) {
			logger.warning("Tried to start next job, but no jobs in queue");
			return false;
		}
		
		activeJob = jobQueue.remove(0);
		activeJob.startJob();
		
		return true;
	}
	
	public Worker createWorker()
	{
		return new Worker(this);
	}
	
	public WebSocket createConsole()
	{	
		return new Console(this);
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


	public void removeWorker(Worker worker) throws WorkerInIllegalStateException
	{
		boolean removed;
		
		synchronized (executeLock) 
		{			
			removed = workers.remove(worker); 
		}
		
		if (!removed) {
			throw new WorkerInIllegalStateException("removeWorker() worker "+worker+" was not registered");
		}
	}
	
	public void addWorker(Worker worker) throws WorkerInIllegalStateException
	{
		synchronized (executeLock) 
		{
			if (workers.contains(worker)) {
				throw new WorkerInIllegalStateException("addWorker() worker "+worker+" already exists");
			}
			
			workers.add(worker);
		}
		
	}
	
	public Job getActiveJob()
	{
		return activeJob;
	}

}
