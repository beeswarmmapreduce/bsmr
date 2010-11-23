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
	
	private boolean isStarted;
	private boolean isFinished;
	
	/* TODO: these variables are not yet set */
	
	private final int splits;
	private final int partitions;
	
	private final long heartbeatTimeout;
	private final long acknowledgeTimeout;
	
	static {
		jobList = new HashMap<Integer, Job>();
	}
	
	
	private Job(int jobId, int splits, int partitions, int heartbeatTimeout, int acknowledgeTimeout)
	{
		this.jobId = jobId;
		this.isStarted = false;
		this.isFinished = false;
		
		this.splits = splits; 
		this.partitions = partitions;
		
		this.heartbeatTimeout   = heartbeatTimeout;
		this.acknowledgeTimeout = acknowledgeTimeout;
	}

	public static Job createJob(int splits, int partitions, int heartbeatTimeout, int acknowledgeTimeout)
	{
		int thisJob;
		synchronized (Job.class) {
			// Monotonic for (2^31-1)/100/60/60/24 days (248.5), one new ID per 1/100th of a second
			thisJob = -1;
			do {
				if (thisJob > 0) { try { Thread.sleep(10); } catch(Exception e) {} }
				thisJob = (int)((System.currentTimeMillis()/100) % Integer.MAX_VALUE);
				
			} while (jobList.containsKey(thisJob));
		}
		Job ret = new Job(thisJob, splits, partitions, heartbeatTimeout, acknowledgeTimeout);
		jobList.put(thisJob, ret); // auto-boxing
		
		return ret;
	}
	
	
	public void startJob()
	{
		splitStore     = new SplitStore(this);
		partitionStore = new PartitionStore(this);
		isStarted      = true;
	}

	public boolean isFinished() { return isFinished; }
	public boolean isStarted()  { return isStarted; }
	
	/**
	 * Marks the job as stopped. Will remove job from the internal data structure
	 * and messages with this job id will not be able to find the job anymore.
	 */
	public void removeJob()
	{
		jobList.remove(jobId);
	}
	
	public void finishJob()
	{
		isFinished = true;
		isStarted = false;
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

	public SplitStore getSplitInformation()
	{
		return splitStore;
	}
	
	public PartitionStore getPartitionInformation()
	{
		return partitionStore;
	}

	public static Job getJobById(int jobId)
	{
		return jobList.get(jobId); // auto-boxing
	}

	public int getJobId()
	{
		return jobId;
	}

	public Object getCode() {
		// TODO Auto-generated method stub
		return null;
	}
 
}
