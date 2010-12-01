package fi.helsinki.cs.bsmr.master;


public class Job 
{
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
	
	private String code;
	
	
	Job(int jobId, int splits, int partitions, int heartbeatTimeout, int acknowledgeTimeout, String code)
	{
		this.jobId = jobId;
		this.isStarted = false;
		this.isFinished = false;
		
		this.splits = splits; 
		this.partitions = partitions;
		
		this.heartbeatTimeout   = heartbeatTimeout;
		this.acknowledgeTimeout = acknowledgeTimeout;
		
		this.code = code;
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
