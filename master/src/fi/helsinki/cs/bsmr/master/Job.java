package fi.helsinki.cs.bsmr.master;


public class Job 
{
	/** Object fields **/
	
	private int jobId;
	
	private SplitStore splitStore;
	private PartitionStore partitionStore;
	
	public enum State {
		NEW,
		RUNNING,
		FINISHED
	};
	
	private State state;
	
	private final int splits;
	private final int partitions;
	
	private final long heartbeatTimeout;
	private final long acknowledgeTimeout;
	
	private long startTime;
	private long finishTime;
	
	private String code;
	
	
	/**
	 * Create a new job
	 * 
	 * @param jobId Unique identifier for this job.
	 * @param splits The number of splits in this job.
	 * @param partitions The number of partitions in this job.
	 * @param heartbeatTimeout The heart beat timeout.
	 * @param acknowledgeTimeout The acknowledgment timeout.
	 * @param code The code the workers are sent.
	 */
	Job(int jobId, int splits, int partitions, int heartbeatTimeout, int acknowledgeTimeout, String code)
	{
		this.jobId = jobId;
		this.state = State.NEW;
		
		this.splits = splits; 
		this.partitions = partitions;
		
		this.heartbeatTimeout   = heartbeatTimeout;
		this.acknowledgeTimeout = acknowledgeTimeout;
		
		this.code = code;
	}
	
	/**
	 * Start this job and initialize the SplitStore and PartitionStore for this job.
	 */
	public void startJob()
	{
		splitStore     = new SplitStore(this);
		partitionStore = new PartitionStore(this);
		state          = State.RUNNING;
		startTime      = TimeContext.now();
	}

	public State getState() { return state; }

	/**
	 * Mark this job as finished.
	 */
	public void finishJob()
	{
		state = State.FINISHED;
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
	
	/**
	 * @return The number of splits in this job.
	 */
	public int getSplits()
	{
		return splits;
	}
	
	/**
	 * @return The number of partitions in this job
	 */
	public int getPartitions()
	{
		return partitions;
	}

	/**
	 * Get more detailed information about the status of all splits in the job.
	 * @return Information about splits.
	 */
	public SplitStore getSplitInformation()
	{
		return splitStore;
	}
	
	/**
	 * Get more detailed information about the status of all partitions in the job.
	 * @return Information about the partitions.
	 */
	public PartitionStore getPartitionInformation()
	{
		return partitionStore;
	}


	public int getJobId()
	{
		return jobId;
	}

	public String getCode() {
		return code;
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
	
	@Override
	public boolean equals(Object obj)
	{
		if (!(obj instanceof Job)) return false;
		
		Job b = (Job)obj;
		return  jobId == b.jobId;
	}
}
