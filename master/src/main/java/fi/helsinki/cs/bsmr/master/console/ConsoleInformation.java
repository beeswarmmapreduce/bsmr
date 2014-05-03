package fi.helsinki.cs.bsmr.master.console;

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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.List;
import java.util.Set;

import fi.helsinki.cs.bsmr.master.JSON;
import fi.helsinki.cs.bsmr.master.Job;
import fi.helsinki.cs.bsmr.master.MasterContext;
import fi.helsinki.cs.bsmr.master.Message;
import fi.helsinki.cs.bsmr.master.Bucket;
import fi.helsinki.cs.bsmr.master.BucketStore;
import fi.helsinki.cs.bsmr.master.Split;
import fi.helsinki.cs.bsmr.master.SplitStore;
import fi.helsinki.cs.bsmr.master.TimeContext;
import fi.helsinki.cs.bsmr.master.Worker;


/**
 * Converts MasterContext information into a status message for consoles.
 * 
 * @author stsavola
 *
 */
public class ConsoleInformation
{
	private String asString;
	
	/**
	 * Create a console information message based on the given master. The master object
	 * needs to be synchronized when the message is created. However, the message
	 * is encoded into String format in the constructor. So there is no need to hold the
	 * master synchronization after the constructor. 
	 * 
	 * @param master The master 
	 */
	public ConsoleInformation(MasterContext master)
	{
		this.asString = createJSONString(master);
	}
	
	public String toJSONString()
	{
		return asString;
	}
	
	private String createJSONString(MasterContext master)
	{
		Job currentJob = master.getActiveJob();
		
		Map<Object, Object> msg = new HashMap<Object, Object>();
		
		msg.put("type","STATUS");
		
		Map<Object, Object> payload = new HashMap<Object, Object>();
		msg.put("payload", payload);
		
		// current timestamp
		payload.put("time", timeToJson(TimeContext.now()));
		
		Map<Object, Object> workers = new HashMap<Object, Object>();
		payload.put("workers", workers);
		for (Worker w : master.getWorkers()) {
			Map<Object, Object> workerInfo = new HashMap<Object, Object>();
			if (currentJob != null) {
				workerInfo.put("status", w.isAvailable(currentJob) ? "available": (w.isDead(currentJob) ? "dead" : "unavailable"));
			} else {
				workerInfo.put("status", "idle");
			}
			workerInfo.put("connectTime", timeToJson(w.getConnectTime()));
			workerInfo.put("url", w.getSocketURL());
			
			workers.put(w.hashCode(), workerInfo);
		}
		
		if (currentJob != null) {
			// Job information
			Map<Object, Object> jobMap = Message.getJSONMapForJob(currentJob);
			jobMap.put("startTime", timeToJson(currentJob.getStartTime()));
			boolean isFinished = currentJob.getState() == Job.State.FINISHED;
			jobMap.put("finished", isFinished);
			if (isFinished) {
				jobMap.put("finishTime", timeToJson(currentJob.getFinishTime()));
			}
			payload.put(Message.FIELD_JOB_MAP, jobMap);
		
			// Job progress
			SplitStore ss = currentJob.getSplitInformation();
			BucketStore bs = currentJob.getBucketInformation();
			
			// Splits
			Map<Object, Object> splitMap = new HashMap<Object, Object>();
			payload.put("splits", splitMap);
			
			
			Map<Object, Object> doneSplits = new HashMap<Object, Object>();
			splitMap.put("done", doneSplits);
			
			Map<Object, Object> queuedSplits = new HashMap<Object, Object>();
			splitMap.put("queued", queuedSplits);
			
			for (int i = 0; i < currentJob.getMapTasks(); i++) {
				Split split = new Split(i);
				
				Set<Worker> tmp;
				
				tmp = ss.getAllWorkersWhoHaveDoneSplit(split);
				if (!tmp.isEmpty()) {
					
					Set<Integer> who = new HashSet<Integer>();
					for (Worker w : tmp) { who.add(w.hashCode()); }
					doneSplits.put(i, who);
				}
				
				tmp = ss.getAllQueuedWorkers(split);
				if (!tmp.isEmpty()) {

					Set<Integer> who = new HashSet<Integer>();
					for (Worker w : tmp) { who.add(w.hashCode()); }
					queuedSplits.put(i, who);
				}
			}
			

			// Buckets
			Map<Object, Object> bucketMap = new HashMap<Object, Object>();
			payload.put("buckets", bucketMap);
			
			// done buckets			
			Map<Object, Object> doneBuckets = new HashMap<Object, Object>();
			bucketMap.put("done", doneBuckets);
			
			for (int i = 0; i < currentJob.getReduceTasks(); i++) {
				Bucket bucket = new Bucket(i);
				
				Set<Integer> who;
				who = bs.getAllDoneWorkers(bucket);
				if (!who.isEmpty()) {
					doneBuckets.put(i, who);
				}
			}
			
			Map<Object, Object> queuedBuckets = new HashMap<Object, Object>();
			bucketMap.put("queued", queuedBuckets);
			
			for (int i = 0; i < currentJob.getReduceTasks(); i++) {
				Bucket bucket = new Bucket(i);
				
				Set<Worker> tmp;
				tmp = bs.getAllQueuedWorkers(bucket);
				if (!tmp.isEmpty()) {
					Set<Integer> who = new HashSet<Integer>();
					for (Worker w : tmp) { who.add(w.hashCode()); }
					queuedBuckets.put(i, who);
				}
			}

		}
		
		
		// Job queue
		List<Map<Object,Object>> jobQueue = new LinkedList<Map<Object, Object>>();
		payload.put("jobQueue", jobQueue);
		for (Job j : master.getJobQueue()) {
			jobQueue.add( Message.getJSONMapForJob(j));
		}
		
		// Job history
		List<Map<Object,Object>> jobHistory = new LinkedList<Map<Object, Object>>();
		payload.put("jobHistory", jobHistory);
		for (Job j : master.getJobHistory()) {
			Map<Object, Object> tmp = Message.getJSONMapForJob(j);
			tmp.put("startTime", timeToJson(j.getStartTime()));
			tmp.put("finishTime", timeToJson(j.getFinishTime()));
			jobHistory.add(tmp);
		}
		
		
		return JSON.toString(msg);
	}
	
	public static long timeToJson(long millis)
	{
		return millis;
		//NOTE: return (int)(millis/1000); <- switch to ints if longs are too wide for JS
	}
}
