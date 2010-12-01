package fi.helsinki.cs.bsmr.master;

import java.io.IOException;
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
			case mapTask: {
				Split s = msg.getMapStatus().split;
				if (s == null || s.getId() < 0 || s.getId() >= activeJob.getSplits()) {
					logger.severe("Worker tried to acknowledge an illegal split "+s+" ("+Message.FIELD_NUM_SPLITS+"="+activeJob.getSplits());
				} else {
					activeJob.getSplitInformation().acknowledgeWork(worker, s);
				}
				break;
			}
				
			case reduceTask: {
				Partition p = msg.getReduceStatus().partition;
				if (p == null || p.getId() < 0 || p.getId() >= activeJob.getPartitions()) {
					logger.severe("Worker tried to acknowledge an illegal partition "+p+" ("+Message.FIELD_NUM_PARTITIONS+"="+activeJob.getPartitions());
				} else {
					activeJob.getPartitionInformation().acknowledgeWork(worker, p);
				}
				break;
			}
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
		
		for (Worker w : getWorkers()) {
			
			try {
				w.sendMessage(pause);
			} catch(IOException ie) {
				logger.log(Level.SEVERE, "Could not pause worker. Terminating connection.", ie);
				
				// NOTE: the callback for onDisconnect() will remove this worker from the worker list
				w.disconnect();
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
			
			Partition p = msg.getReduceStatus().partition;
			Split s     = msg.getReduceStatus().split;
			
			boolean ok = true;
			
			if (p == null || p.getId() < 0 || p.getId() >= activeJob.getPartitions()) {
				ok = false;
			}
			
			if (s == null || s.getId() < 0 || s.getId() >= activeJob.getSplits()) {
				ok = false;
			}
			
			if (ok) {
				return Message.findSplitAtMessage(msg.getReduceStatus().partition, msg.getReduceStatus().split, activeJob, msg.getUnareachableWorkers());
			}

		}
			
		// All splits are done => Assign a partition for reducing  (or the client specified a non-valid partition or split
			
		Partition nextPartition = activeJob.getPartitionInformation().selectPartitionToWorkOn(worker);
			
		return Message.reduceThatMessage(nextPartition, activeJob);
	}

}
