package fi.helsinki.cs.bsmr.master;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;


public class Job 
{
	private static Logger logger = Util.getLoggerForClass(Job.class); 
	
	private static Map<Integer, Job> jobMap;
	
	/** Object fields **/
	
	private int jobId;
	
	private SplitStore splitStore;
	private PartitionStore partitionStore;
	
	private boolean isStarted;
	private boolean isFinished;
	
	private final int splits;
	private final int partitions;
	
	private final long heartbeatTimeout;
	private final long acknowledgeTimeout;
	
	private long startTime;
	private long finishTime;
	
	static {
		jobMap = new HashMap<Integer, Job>();
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
				
			} while (jobMap.containsKey(thisJob));
		}
		
		Job ret = new Job(thisJob, splits, partitions, heartbeatTimeout, acknowledgeTimeout);
		logger.info("Created new Job "+ret);
		jobMap.put(thisJob, ret); // auto-boxing
		
		return ret;
	}
	
	
	public void startJob()
	{
		splitStore     = new SplitStore(this);
		partitionStore = new PartitionStore(this);
		isStarted      = true;
		startTime      = TimeContext.now();
	}

	public boolean isFinished() { return isFinished; }
	public boolean isStarted()  { return isStarted; }
	
	/**
	 * Marks the job as stopped. Will remove job from the internal data structure
	 * and messages with this job id will not be able to find the job anymore.
	 */
	public void removeJob()
	{
		jobMap.remove(jobId);
	}
	
	public void finishJob()
	{
		isFinished = true;
		isStarted  = false;
		finishTime = TimeContext.now();
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
		return jobMap.get(jobId); // auto-boxing
	}

	public int getJobId()
	{
		return jobId;
	}

	public Object getCode() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public long getStartTime()
	{
		return startTime;
	}
	
	public long getFinishTime()
	{
		return finishTime;
	}
 
	public String toString()
	{
		return "Job#"+jobId+" (M="+splits+", R="+partitions+")";
	}
}
