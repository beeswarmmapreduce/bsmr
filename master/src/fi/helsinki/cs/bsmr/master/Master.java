package fi.helsinki.cs.bsmr.master;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.jetty.websocket.WebSocket;

import fi.helsinki.cs.bsmr.master.console.Console;


public class Master extends WorkerStore
{
	private static Logger logger = Util.getLoggerForClass(Master.class);
	
	/**
	 * Needs to be visible for Worker (and possible Console)
	 */ 
	
	private List<Job> jobQueue;
	private List<Job> jobHistory;
	private Job activeJob;
	
	
	public Master()
	{
		activeJob  = null;
		jobQueue   = new LinkedList<Job>();
		jobHistory = new LinkedList<Job>();
	}

	public synchronized void queueJob(Job j) throws JobAlreadyRunningException
	{
		if (j.isStarted() || j.isFinished()) {
			logger.severe("Tried to add job "+j+" to job queue, but it's in an illegal state (started or finished)");
			throw new JobAlreadyRunningException("Tried to add job "+j+" to job queue, but it's in an illegal state (started or finished)");
		}
		
		jobQueue.add(j);
	}
	
	public List<Job> getJobQueue()
	{
		return Collections.unmodifiableList(jobQueue);
	}
	
	public synchronized boolean startNextJob() throws JobAlreadyRunningException
	{
		
		if (activeJob != null && !activeJob.isFinished()) {
			logger.severe("Tried to start next job, but we have a non-finished active job!");
			throw new JobAlreadyRunningException("Tried to start next job, but we have a non-finished active job!");
		}
		
		if (jobQueue.isEmpty()) {
			logger.fine("Tried to start next job, but no jobs in queue");
			return false;
		}
		
		if (activeJob != null) {
			jobHistory.add(activeJob);
		}
		
		activeJob = jobQueue.remove(0);
		activeJob.startJob();
		
		Set<Worker> badWorkers = null;
		
		Message dummyStatus = Message.pauseMessage();
		
		for (Worker w : workers) {
			// Skip inactive workers because they might be dead and communicating with them would slow down operations
			// if IO is async, this is not necessary! But.. Looking at Jetty 8.0.0.M2 sources, it seems (although
			// it is difficult to verify) that sendMessage() is in fact synchronous.
			if (!w.isAvailable(activeJob)) continue;
			
			Message msg = selectTaskForWorker(w, dummyStatus);
			
			try {
				w.sendMessage(msg);
			} catch(IOException ie) {
				logger.log(Level.SEVERE, "Could not send work to worker. Terminating connection.", ie);
				if (badWorkers == null) badWorkers = new HashSet<Worker>();
				
				badWorkers.add(w);
			}
		}

		if (badWorkers != null) {
			for (Worker bad : badWorkers) {
				try {
					removeWorker(bad);
				} catch(WorkerInIllegalStateException wiise) {
					logger.log(Level.SEVERE, "Bad worker could not be removed?.. perhaps it was removed by a callback", wiise);
				} finally {
					bad.disconnect();
				}
			}
		}
		
		return true;
	}
	
	private synchronized void finishCurrentJobAndStartNext()
	{
		activeJob.finishJob();
		
		// if there is no next job, this block will pause all workers
		// if there is a next job, startNextJob() will send out initial work
		//  => this function should always 
		boolean newWorkStarted;
		
		try {
			// startNextJob sends out messages to workers, 
			newWorkStarted = startNextJob();
		} catch(JobAlreadyRunningException jare) {
			logger.log(Level.SEVERE,"Programming error or concurrency issue! the previous active job was marked as finished and the attempt to start the next warns that there is a job already running!", jare);
			System.exit(1);
			return; // Never reached
		}
		
		if (!newWorkStarted) {
			pauseAllWorkers();
		}
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
	public synchronized boolean acknowledgeWork(Worker worker, Message msg)
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
		
		// If the job is not yet marked as finished
		// !activeJob.isFinished() is documentation. we don't execute this function if the job is finished
		if (allPartitionsDone && !activeJob.isFinished()) { 
			finishCurrentJobAndStartNext();
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
					logger.log(Level.SEVERE, "Bad worker could not be removed?.. perhaps it was removed by a callback", wiise);
				} finally {
					bad.disconnect();
				}
			}
		}
	}

	/**
	 * Executes a worker message. Marks done splits or partitions and assigns new work.
	 * This method expects to receive an ACK message with the correct job id. The worker
	 * is already synchronized on the master, but the same thread can sync on the 
	 * same object multiple times. Thus it is better to have the synchronization here
	 * as well to keep the code coherent.
	 * 
	 * @param worker The worker who sent the message
	 * @param msg The message
	 * @return Reply message to the worker
	 */
	public synchronized Message selectTaskForWorker(Worker worker, Message msg)
	{	

		// All partitions are not done yet, but let's first check the splits:
		if (!activeJob.getSplitInformation().areAllSplitsDone(msg.getUnareachableWorkers())) {
			// Assign a map task
			Split nextSplit = activeJob.getSplitInformation().selectSplitToWorkOn(worker, msg.getUnareachableWorkers());
			
			return Message.mapThisMessage(nextSplit, activeJob);
		} 
		
		if (msg.getJob() == activeJob && 
		    msg.getAction() == Message.Action.reduceSplit && 
			!activeJob.getPartitionInformation().isPartitionDone(msg.getIncompleteReducePartition())) { 
			
			return Message.findSplitAtMessage(msg.getReduceStatus().partition, msg.getReduceStatus().split, activeJob, msg.getUnareachableWorkers());

		} else {
			
			// All splits are done => Assign a partition for reducing
			
			Partition nextPartition = activeJob.getPartitionInformation().selectPartitionToWorkOn(worker);
			
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

	public List<Job> getJobHistory() 
	{
		return Collections.unmodifiableList(jobHistory);
	}

}
