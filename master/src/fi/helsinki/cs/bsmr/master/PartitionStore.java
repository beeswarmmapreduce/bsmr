package fi.helsinki.cs.bsmr.master;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class PartitionStore
{
	private List<Set<Worker>> partitionsQueued;
	private boolean []partitionsDone;
	private boolean allPartitionsDone; 
	
	private List<Partition> workQueue;
	private int previousIndexOfWorkQueue;
	
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
	
	public boolean areAllPartitionsDone()
	{
		return allPartitionsDone;
	}
	
	public Partition selectPartitionToWorkOn(Worker toWhom)
	{
		previousIndexOfWorkQueue = (previousIndexOfWorkQueue + 1) % workQueue.size();

		Partition work = workQueue.get(previousIndexOfWorkQueue);
		
		partitionsQueued.get(work.getId()).add(toWhom);
		
		return work;
	}

	/**
	 * Partition work is permanent. When an acknowledgment is received, that partition is saved to the
	 * central FS. Thus we do not keep state of who has done what.
	 * @return
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

	public boolean isPartitionDone(Partition partition) 
	{
		return partitionsDone[partition.getId()];
	}
	
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

	public Set<Worker> getAllQueuedWorkers(Partition partition)
	{
		return Collections.unmodifiableSet(partitionsQueued.get(partition.getId()));
	}
}
