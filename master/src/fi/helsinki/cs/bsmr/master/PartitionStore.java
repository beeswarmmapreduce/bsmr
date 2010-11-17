package fi.helsinki.cs.bsmr.master;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PartitionStore
{
	private List<Set<Worker>> partitionsQueued;
	private Set<Partition> partitionsDone;
	
	private Job job;
	
	private List<Partition> workQueue;
	private int workQueuePointer;
	
	public PartitionStore(Job job)
	{
		this.partitionsQueued = new ArrayList<Set<Worker>>();
		this.workQueue = new ArrayList<Partition>();
		this.workQueuePointer = -1;
		
		for (int i = 0; i < job.getPartitions(); ++i) {
			partitionsQueued.add(new HashSet<Worker>());
			workQueue.add(new Partition(i));
		}
		
		this.partitionsDone = new HashSet<Partition>();
		
		this.job = job;
		
	}
	
	public boolean areAllPartitionsDone()
	{
		return (partitionsDone.size() == job.getPartitions());
	}
	
	public Partition selectPartitionToWorkOn()
	{
		workQueuePointer = (workQueuePointer + 1) % workQueue.size();

		return workQueue.get(workQueuePointer);
	}

	/**
	 * Partition work is permanent. When an acknowledgment is received, that partition is saved to the
	 * central FS. Thus we do not keep state of who has done what.
	 * @return
	 */
	public void acknowledgeWork(Worker w, Partition p)
	{
		if (!this.partitionsDone.contains(p)) {
			partitionsDone.add(p);
		}
		
		partitionsQueued.get(p.getId()).remove(w);
		
		workQueue.remove(p);
	}

	public boolean isPartitionDone(Partition partition) 
	{
		return partitionsDone.contains(partition);
	}
}
