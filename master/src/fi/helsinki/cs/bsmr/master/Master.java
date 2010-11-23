package fi.helsinki.cs.bsmr.master;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.jetty.websocket.WebSocket;


public class Master extends WorkerStore
{
	private static Logger logger = Util.getLoggerForClass(Master.class);
	
	/**
	 * Needs to be visible for Worker (and possible Console)
	 */ 
	
	private List<Job> jobQueue;
	private Job activeJob;
	
	
	
	public Master()
	{
		activeJob   = null;
		jobQueue    = new LinkedList<Job>();
		workerURLs  = new HashMap<String, Worker>();
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

	public WebSocket createConsole()
	{	
		return new Console(this);
	}
	
	public Object executeConsoleMessage(Console console, Object foo)
	{
		Object ret;
		
		synchronized (this) {
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
		synchronized (this) 
		{	
			if (msg.getJob() == activeJob) {
				// acknowledge data from worker
				switch (msg.getAction()) {
				case mapTask:
					activeJob.getSplitInformation().acknowledgeWork(worker, msg.getMapStatus().split);
					break;
					
				case reduceTask:
					activeJob.getPartitionInformation().acknowledgeWork(worker, msg.getReduceStatus().partition);
					break;
				}
			}

			// TODO: mark unavailable URLS /* FOR THIS REQUEST ONLY */
			
			
			
			// Check if all partitions are reduced
			boolean allPartitionsDone = activeJob.getPartitionInformation().areAllPartitionsDone();
			
			// If the job is not yet marked as finished
			// !activeJob.isFinished() == Documentation. we don't execute this function if the job is finished
			if (allPartitionsDone && !activeJob.isFinished()) { 
				activeJob.finishJob();
				
				Message pause = Message.pauseMessage(activeJob); 
				
				Set<Worker> badWorkers = null;
				
				for (Worker w : workers) {
					if (w == worker) continue;
					
					try {
						w.sendMessage(pause);
					} catch(IOException ie) {
						logger.log(Level.SEVERE, "Could not pause worker. Terminating connection.", ie);
						if (badWorkers == null) badWorkers = new HashSet<Worker>();
						
						badWorkers.add(w);
					}	
				}
				
				if (badWorkers != null) {
					for (Worker bad : badWorkers) {
						try {
							removeWorker(bad);
						} catch(WorkerInIllegalStateException wiise) {
							logger.log(Level.SEVERE, "Bad worker could not be removed?..", wiise);
						} finally {
							bad.disconnect();
						}
					}
				}
				
				return pause; // this sends the message to "worker"
			}
			
			
			// All partitions are not done yet, but let's first check the splits:
			if (!activeJob.getSplitInformation().areAllSplitsDone(msg.getUnareachableWorkers())) {
				// Assign a map task
				Split nextSplit = activeJob.getSplitInformation().selectSplitToWorkOn(msg.getUnareachableWorkers());
				
				return Message.mapThisMessage(nextSplit, activeJob);
			} 
			
			// TODO: check if the worker is already reducing and is asking for a bsURL
			if (msg.getJob() == activeJob && 
			    msg.getAction() == Message.Action.reduceSplit && 
				!activeJob.getPartitionInformation().isPartitionDone(msg.getIncompleteReducePartition())) { 
				
				
				return null;
			} else {
				
				// All splits are done => Assign a partition for reducing
				
				Partition nextPartition = activeJob.getPartitionInformation().selectPartitionToWorkOn();
				
				return Message.reduceThatMessage(nextPartition, activeJob);
			}
		
			
		}
	
	}
	
	public Job getActiveJob()
	{
		return activeJob;
	}
	
	public boolean isActive()
	{
		return activeJob != null && !activeJob.isFinished() && activeJob.isStarted();
	}

}
