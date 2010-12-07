package fi.helsinki.cs.bsmr.master;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SplitStore implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	private List<Set<Worker>> splitsDone;
	private List<Set<Worker>> splitsQueued;
	private Job job;
	

	private int workQueuePointer;
	
	public SplitStore(Job job)
	{
		
		this.splitsDone   = new ArrayList<Set<Worker>>();
		this.splitsQueued = new ArrayList<Set<Worker>>();
		
		this.workQueuePointer = -1;
		
		for (int i = 0; i < job.getSplits(); ++i) {
			splitsDone.add(new HashSet<Worker>());
			splitsQueued.add(new HashSet<Worker>());
		}
		
		this.job = job;
	}
	
	public Set<Worker> whoHasSplit(Split s)
	{
		return new AvailableWorkerSet(splitsDone.get(s.getId()), job);
	}
	

	public Split selectSplitToWorkOn(Worker toWhom, Set<Worker> unreachableWorkers)
	{
		int i = 0;
		Split ret;
		
		
		boolean isGoodCandidate;
		
		do {
			workQueuePointer = (workQueuePointer + 1) % job.getSplits();
			ret = new Split(workQueuePointer);
			i++;
			
			Set<Worker> workersWhoHaveSplit = whoHasSplit(ret); 
			isGoodCandidate = workersWhoHaveSplit.isEmpty() || unreachableWorkers.containsAll(workersWhoHaveSplit);
			
		} while (!isGoodCandidate && i < job.getSplits());
	
		if (!isGoodCandidate) {
			return null;
		}
		
		splitsQueued.get(ret.getId()).add(toWhom);
		
		return ret;
	}
	
	/**
	 * Mark split s as done in worker w and unmark it as queued.
	 * 
	 * @param w The worker
	 * @param s The split
	 */
	public void acknowledgeWork(Worker w, Split s)
	{
		Set<Worker> split = splitsDone.get(s.getId());
		
		if (!split.contains(w)) {
			split.add(w);
		}
		
		splitsQueued.get(s.getId()).remove(w);
	}
	
	/**
	 * Actually remove all data marked for the worker.
	 * 
	 * @param w The worker to be removed
	 */
	public void workerTrulyRemoved(Worker w)
	{
		for (int i = 0; i < job.getSplits(); ++i) {
			splitsDone.get(i).remove(w);
			splitsQueued.get(i).remove(w);
		}
	}
	
	public Set<Worker> getAllWorkersWhoHaveDoneSplit(Split s)
	{
		return Collections.unmodifiableSet(splitsDone.get(s.getId()));
	}
	
	public Set<Worker> getAllQueuedWorkers(Split s)
	{
		return Collections.unmodifiableSet(splitsQueued.get(s.getId()));
	}
}
