package fi.helsinki.cs.bsmr.master;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;


public class MasterImpl extends MasterStoreImpl
{
	private static Logger logger = Util.getLoggerForClass(MasterImpl.class);
		
	private synchronized void finishCurrentJobAndStartNext()
	{
		getActiveJob().finishJob();
		
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
		Job activeJob = getActiveJob();
		
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
		
		for (Worker w : getWorkers()) {
			
			try {
				w.sendMessage(pause);
			} catch(IOException ie) {
				logger.log(Level.SEVERE, "Could not pause worker. Terminating connection.", ie);
				if (badWorkers == null) badWorkers = new HashSet<Worker>();
				
				badWorkers.add(w);
			}	
		}
		
		// TODO: this might be in vain, we could just call disconnect() on the bad worker
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
		Job activeJob = getActiveJob();
		
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

}
