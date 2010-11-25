package fi.helsinki.cs.bsmr.master;

import java.io.IOException;
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
			logger.fine("Tried to start next job, but no jobs in queue");
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
	 * Acknowledges work from the worker. If the message acknowledges the last part of the current job, it will start the next job.
	 * If no new jobs are queued, this will send idle messages to all workers and return false. 
	 * 
	 * @note This function can switch the active job!
	 * 
	 * @param worker The worker who is acknowledging work
	 * @param msg The message
	 * @return True if there is work to be done, false if the worker should be idle.
	 */
	public boolean acknowledgeWork(Worker worker, Message msg)
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
	
		// Check if all partitions are reduced
		boolean allPartitionsDone = activeJob.getPartitionInformation().areAllPartitionsDone();
		
		boolean thereIsWorkToBeDone = true;
		
		// If the job is not yet marked as finished
		// !activeJob.isFinished() is documentation. we don't execute this function if the job is finished
		if (allPartitionsDone && !activeJob.isFinished()) { 
			activeJob.finishJob();
			
			try {
				thereIsWorkToBeDone = startNextJob();
			} catch(JobAlreadyRunningException jare) {
				logger.log(Level.SEVERE,"Programming error or concurrency issue! the previous active job was marked as finished and the attempt to start the next warns that there is a job already running!", jare);
				return false;
			}
			
		}
		
		if (!thereIsWorkToBeDone) 
		{
			// This is executed only when there was no new job in the job queue
			pauseAllWorkers();
			return false;
		}
	
		return true;
	}

	private void pauseAllWorkers() 
	{
		Message pause = Message.pauseMessage(); 
		
		Set<Worker> badWorkers = null;
		
		for (Worker w : workers) {
			
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
	public Message selectTaskForWorker(Worker worker, Message msg)
	{	

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
			
			return Message.findSplitAtMessage(msg.getReduceStatus().partition, msg.getReduceStatus().split, activeJob, msg.getUnareachableWorkers());

		} else {
			
			// All splits are done => Assign a partition for reducing
			
			Partition nextPartition = activeJob.getPartitionInformation().selectPartitionToWorkOn();
			
			return Message.reduceThatMessage(nextPartition, activeJob);
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
