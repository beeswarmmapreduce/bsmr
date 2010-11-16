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
	
	/**
	 * Needs to be visible for Worker (and possible Console)
	 */
	final Object executeLock; 
	
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
	
	public Object executeConsoleMessage(Console console, Object foo)
	{
		Object ret;
		
		synchronized (executeLock) {
			ret = null;
		}
		
		return ret;
	}

	/**
	 * Executes a worker message. Marks done splits or partitions and assigns new work.
	 * This method expects to receive an ACK message with the correct job id. The worker
	 * is already synchronized on executeLock, but the same thread can sync on the 
	 * same object multiple times. Thus it is better to have the synchronization here
	 * as well to keep the code coherent.
	 * 
	 * @param worker The worker who sent the message
	 * @param msg The message
	 * @return Reply message to the worker
	 */
	public Message executeWorkerMessage(Worker worker, Message msg)
	{	
		// msg will be of type ACK. worker checks this
		synchronized (executeLock) 
		{
			if (msg.getAction() == Message.Action.up) {
				// TODO: what?
			}
			
			// TODO: Acknowledge the data given in the message
			
			// TODO: mark unavailable workers (set heartbeat to (now - 2*heartbeat))
			
			
			
			// Check if all partitions are reduced
			boolean allPartitionsDone = activeJob.getPartitionInformation().areAllPartitionsDone();
			
			// If the job is not yet marked as finished
			if (allPartitionsDone && !activeJob.isFinished()) {
				activeJob.finishJob();
				// TODO: what to do next?
			}
			
			// The job is finished => pause the worker
			if (activeJob.isFinished()) {
				return Message.pauseMessage();
			}
			
			// All partitions are not done yet, but let's first check the splits:
			if (!activeJob.getSplitInformation().areAllSplitsDone()) {
				// Assign a map task
				Split nextSplit = activeJob.getSplitInformation().selectSplitToWorkOn();
				
				return Message.mapThisMessage(nextSplit, activeJob);
				
			} else {
				// All splits are done => Assign a partition
				
				Partition nextPartition = activeJob.getPartitionInformation().selectPartitionToWorkOn();
				
				return Message.reduceThatMessage(nextPartition, activeJob);
			}
			
		}
	
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
