package fi.helsinki.cs.bsmr.master;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import fi.helsinki.cs.bsmr.master.console.ConsoleInformation;

public class PartitionStore implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	private List<Set<Worker>> partitionsQueued;
	private boolean []partitionsDone;
	private boolean allPartitionsDone; 
	
	private List<Partition> workQueue;
	private int previousIndexOfWorkQueue;
	
	/**
	 * Create a new PartitionStore for the job. The Job is only used for initializing the
	 * data structures and is not needed to assess the state of workers.
	 * 
	 * @param job The job this PartitionStore is for
	 */
	public PartitionStore(Job job)
	{
		this.partitionsQueued = new ArrayList<Set<Worker>>();
		this.workQueue = new LinkedList<Partition>();
		this.previousIndexOfWorkQueue = -1;
		
		for (int i = 0; i < job.getPartitions(); ++i) {
			partitionsQueued.add(new HashSet<Worker>());
			workQueue.add(new Partition(i));
		}
		
		this.partitionsDone = new boolean[job.getPartitions()];
		this.allPartitionsDone = false;
	}
	
	/**
	 * Tests whether all partitions are reduced.
	 * 
	 * @return True if all partitions are reduced and false if otherwise. 
	 */
	public boolean areAllPartitionsDone()
	{
		return allPartitionsDone;
	}
	
	/**
	 * Select next partition to reduce. The partition is selected by selecting a partition
	 * from a work queue of non-reduced partitions. The PartitionStore has an index pointing to the
	 * previously selected partition that is updated when this call is made. The selected
	 * partition will be set as queued for the worker in question.
	 * 
	 * @param toWhom The worker we are selecting a Partition for.
	 * @return The next partition to work on.
	 */
	public Partition selectPartitionToWorkOn(Worker toWhom)
	{
		previousIndexOfWorkQueue = (previousIndexOfWorkQueue + 1) % workQueue.size();

		Partition work = workQueue.get(previousIndexOfWorkQueue);
		
		partitionsQueued.get(work.getId()).add(toWhom);
		
		return work;
	}

	/**
	 * Acknowledge that a worker has reduced a partition. We do not save who has reduced which partition
	 * as that information is not required as the resulting data has been saved to the FS before acknowledgment.
	 * The acknowledged partition is removed from the work queue and the queue index is updated as necessary.
	 * The method also updates the worker queue for the partition in question (by removing this worker from the
	 * queue). 
	 * 
	 * @param w The worker who is acknowledging work
	 * @param p The partition that is being acknowledged
	 */
	public void acknowledgeWork(Worker w, Partition p)
	{
		if (!this.partitionsDone[p.getId()]) {
			this.partitionsDone[p.getId()] = true;
			boolean test = true;
			for (boolean b : this.partitionsDone) {
				if (!b) { test = false; break; }
			}
			if (test) {
				allPartitionsDone = true;
			}
		}
		
		partitionsQueued.get(p.getId()).remove(w);
		
		// Remove partition p if it's in the work queue
		int i = workQueue.indexOf(p);
		if (i != -1) {
			workQueue.remove(i);
			// And fix work queue indexing if necessary
			if (i <= previousIndexOfWorkQueue) {
				previousIndexOfWorkQueue--;
			}
		}
			
			
	}
	
	/**
	 * Remove all data marked for the worker. Note that any data acknowledged by the
	 * worker has been saved to the FS, so there is no need to de-acknowledge them.
	 * 
	 * @param w The worker to be removed
	 */
	public void removeWorkerInformation(Worker w)
	{
		for (Set<Worker> workers : partitionsQueued) {
			workers.remove(w);
		}
	}

	/**
	 * Tells us whether a specific partition has been reduced or not.
	 * 
	 * @param partition The partition in question.
	 * @return True if the partition has been reduced, false if otherwise
	 */
	public boolean isPartitionDone(Partition partition) 
	{
		return partitionsDone[partition.getId()];
	}
	
	/**
	 * Calculate ranges of partitions which have been reduced. This is only needed by the Console
	 * interface.
	 *  
	 * @return A List of Lists of Integers. Each sublist contains two integers: the start and 
	 *         the end of the reduced range. Both the start and end are inclusive.   
	 */
	public List<List<Integer>> getDonePartitionRanges()
	{
		List<List<Integer>> ret = new LinkedList<List<Integer>>();
		
		int start = -1;
		int end = -1;
		
		int i = 0;
		
		
		// For each "true" value, end is updated
		for (boolean b : partitionsDone) {
			
			if (b) {
				// update the end accordingly
				end = i;
			}
		
			if (b && start == -1) {
				// start of new range
				start = i;
				
			} else if (!b && start != -1) {
				// if we find a "false" value after a non-false value, we
				// add the range
				List<Integer> range = new LinkedList<Integer>();
				range.add(start);
				range.add(end);
				ret.add(range);
				
				start = end = -1; // reset range
			}
			
			i++;
		}
		// if the range continued to the end of the list
		if (start != -1) {
			List<Integer> range = new LinkedList<Integer>();
			range.add(start);
			range.add(end);
			ret.add(range);
		}
		
		return ret;
	}

	/**
	 * Returns all workers for whom a partition has been queued for.
	 * 
	 * @param partition The partition in question
	 * @return An unmodifiable set of workers for whom the partition has been queued
	 */
	public Set<Worker> getAllQueuedWorkers(Partition partition)
	{
		return Collections.unmodifiableSet(partitionsQueued.get(partition.getId()));
	}
}
