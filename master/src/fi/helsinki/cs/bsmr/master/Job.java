package fi.helsinki.cs.bsmr.master;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;


public class Job 
{
	private static Logger logger = Util.getLoggerForClass(Job.class); 
	
	/** Job counter, access only via synchronizing the class Job **/
	private static int jobCounter = 0;
	private static Map<Integer, Job> jobList;
	
	/** Object fields **/
	
	private int jobId;
	
	private SplitStore splitStore;
	private PartitionStore partitionStore;
	
	/* TODO: these variables are not yet set */
	
	private final int splits;
	private final int partitions;
	
	private final long heartbeatTimeout;
	private final long acknowledgeTimeout;
	
	static {
		jobList = new HashMap<Integer, Job>();
	}
	
	
	private Job(int jobId)
	{
		this.jobId = jobId;
		
		this.splits = 100; 
		this.partitions = 100;
		
		this.heartbeatTimeout   = 60000;
		this.acknowledgeTimeout = this.heartbeatTimeout * 60;
	}

	public static Job createJob()
	{
		int thisJob;
		synchronized (Job.class) {
			thisJob = jobCounter++;
		}
		Job ret = new Job(thisJob);
		jobList.put(thisJob, ret); // auto-boxing
		
		return ret;
	}
	
	
	public void startJob()
	{
		splitStore     = new SplitStore(this);
		partitionStore = new PartitionStore(this);
	}

	/**
	 * Marks the job as stopped. Will remove job from the internal data structure
	 * and messages with this job id will not be able to find the job anymore.
	 */
	public void endJob()
	{
		jobList.remove(jobId);
	}
	
	public long getWorkerHeartbeatTimeout()
	{
		return heartbeatTimeout;
	}
	
	public long getWorkerAcknowledgeTimeout()
	{
		return acknowledgeTimeout;
	}
	
	public int getSplits()
	{
		return splits;
	}
	
	public int getPartitions()
	{
		return partitions;
	}


	public static Job getJobById(int jobId)
	{
		return jobList.get(jobId); // auto-boxing
	}

	public int getJobId()
	{
		return jobId;
	}
 
}
