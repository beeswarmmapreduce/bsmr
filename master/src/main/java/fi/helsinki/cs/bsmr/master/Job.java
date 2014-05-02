package fi.helsinki.cs.bsmr.master;

/**
 * The MIT License
 * 
 * Copyright (c) 2010   Department of Computer Science, University of Helsinki
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * Author Sampo Savolainen
 *
 */

/**
 * Describes one job for the BSMR cluster.
 * 
 * @author stsavola
 */
public class Job
{
/** Object fields **/

private int jobId;

private SplitStore splitStore;
private BucketStore bucketStore;

public enum State
{
NEW, RUNNING, FINISHED
};

private State state;

private final int maptasks;
private final int reducetasks;

private final long heartbeatTimeout;
private final long acknowledgeTimeout;

private long startTime;
private long finishTime;

private Object code;

/**
 * Create a new job
 * 
 * @param jobId
 *            Unique identifier for this job.
 * @param maptasks
 *            The number of maptasks in this job.
 * @param reducetasks
 *            The number of reducetasks in this job.
 * @param heartbeatTimeout
 *            The heart beat timeout.
 * @param acknowledgeTimeout
 *            The acknowledgment timeout.
 * @param code
 *            The code the workers are sent.
 */
Job(int jobId, int maptasks, int reducetasks, int heartbeatTimeout,
		int acknowledgeTimeout, Object code)
	{
	this.jobId = jobId;
	this.state = State.NEW;

	this.maptasks = maptasks;
	this.reducetasks = reducetasks;

	this.heartbeatTimeout = heartbeatTimeout;
	this.acknowledgeTimeout = acknowledgeTimeout;

	this.code = code;
	}

/**
 * Start this job and initialize the SplitStore and BucketStore for this job.
 */
public void startJob()
	{
	splitStore = new SplitStore(this);
	bucketStore = new BucketStore(this);
	state = State.RUNNING;
	startTime = TimeContext.now();
	}

public State getState()
	{
	return state;
	}

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
public int getMapTasks()
	{
	return maptasks;
	}

/**
 * @return The number of reduce tasks in this job
 */
public int getReduceTasks()
	{
	return reducetasks;
	}

/**
 * Get more detailed information about the status of all splits in the job.
 * 
 * @return Information about splits.
 */
public SplitStore getSplitInformation()
	{
	return splitStore;
	}

/**
 * Get more detailed information about the status of all buckets in the job.
 * 
 * @return Information about the buckets.
 */
public BucketStore getBucketInformation()
	{
	return bucketStore;
	}

public int getJobId()
	{
	return jobId;
	}

public Object getCode()
	{
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
	return "Job#" + jobId + " (M=" + maptasks + ", R=" + reducetasks + ")";
	}

@Override
public boolean equals(Object obj)
	{
	if (!(obj instanceof Job))
		return false;

	Job b = (Job) obj;
	return jobId == b.jobId;
	}
}
