package fi.helsinki.cs.bsmr.master;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Keeps the state of splits for a single job.
 * 
 * @author stsavola
 *
 */
public class SplitStore implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	private List<Set<Worker>> splitsDone;
	private List<Set<Worker>> splitsQueued;
	private Job job;
	

	private int workQueuePointer;
	
	/**
	 * Create a new SplitStore for the job. The Job controls the number of splits and the availability of workers.
	 *  
	 * @param job The Job this SplitStore is created for
	 */
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
	
	/**
	 * Provide a set of available Workers who have calculated a split. the availability of the workers depends on the
	 * Job this SplitStore has been created for. 
	 * 
	 * @param s The split we want workers for
	 * @return A set of available workers.
	 */
	public Set<Worker> whoHasSplit(Split s)
	{
		return new AvailableWorkerSet(splitsDone.get(s.getId()), job);
	}
	

	/**
	 * Finds next split to work on. It skips splits that have already been calculated by other workers. Workers who
	 * have become unavailable (or dead) or workers specified as unreachable are taken into considered. The SplitStore
	 * keeps a pointer to the work queue so that subsequent calls will provide different sets.
	 * 
	 * If all splits have been calculated by available and reachable workers, this method will return a null split. 
	 * 
	 * @param toWhom Which worker is asking for a split. The selected split will be set as queued for this worker.
	 * @param unreachableWorkers Workers that the caller cannot contact. These workers need to be considered as unavailable.
	 * @return The split to work on or null if all splits have been calculated.
	 */
	public Split selectSplitToWorkOn(Worker toWhom, Set<Worker> unreachableWorkers)
	{
		int i = 0;
		Split ret;
		
		
		boolean foundGoodCandidate;
		
		do {
			workQueuePointer = (workQueuePointer + 1) % job.getSplits();
			ret = new Split(workQueuePointer);
			i++;
			
			Set<Worker> workersWhoHaveSplit = whoHasSplit(ret); 
			foundGoodCandidate = workersWhoHaveSplit.isEmpty() || unreachableWorkers.containsAll(workersWhoHaveSplit);
			
		} while (!foundGoodCandidate && i < job.getSplits());
	
		if (!foundGoodCandidate) {
			return null;
		}
		
		splitsQueued.get(ret.getId()).add(toWhom);
		
		return ret;
	}
	
	/**
	 * Mark split as calculated by worker and remove it from the queue.
	 * 
	 * @param w The worker who is acknowledging work.
	 * @param s The split to be acknowledged
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
	 * Remove all data marked for the worker.
	 * 
	 * @param w The worker to be removed
	 */
	public void removeWorkerInformation(Worker w)
	{
		for (int i = 0; i < job.getSplits(); ++i) {
			splitsDone.get(i).remove(w);
			splitsQueued.get(i).remove(w);
		}
	}
	
	/**
	 * Retrieve a set of workers who have calculated a split regardless of the status of the workers.
	 * The returned set might contain unavailable or dead workers.
	 * 
	 * @param s The split we are interested in.
	 * @return An unmodifiable set of workers.
	 */
	public Set<Worker> getAllWorkersWhoHaveDoneSplit(Split s)
	{
		return Collections.unmodifiableSet(splitsDone.get(s.getId()));
	}
	
	/**
	 * Retrieve a set of workers for whom the split in question has been queued regardless of the status
	 * of the workers. The returned set might contain unavailable or dead workers.
	 * 
	 * @param s The split we are interested in.
	 * @return An unmodifiable set of workers.
	 */
	public Set<Worker> getAllQueuedWorkers(Split s)
	{
		return Collections.unmodifiableSet(splitsQueued.get(s.getId()));
	}
}
