package fi.helsinki.cs.bsmr.master.console;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.List;
import java.util.Set;

import org.eclipse.jetty.util.ajax.JSON;

import fi.helsinki.cs.bsmr.master.Job;
import fi.helsinki.cs.bsmr.master.Master;
import fi.helsinki.cs.bsmr.master.Message;
import fi.helsinki.cs.bsmr.master.Partition;
import fi.helsinki.cs.bsmr.master.PartitionStore;
import fi.helsinki.cs.bsmr.master.Split;
import fi.helsinki.cs.bsmr.master.SplitStore;
import fi.helsinki.cs.bsmr.master.TimeContext;
import fi.helsinki.cs.bsmr.master.Worker;


public class ConsoleInformation
{
	private Master master;
	
	public ConsoleInformation(Master master)
	{
		this.master = master;
	}
	
	public String toJSONString()
	{
		Job currentJob = master.getActiveJob();
		
		Map<Object, Object> msg = new HashMap<Object, Object>();
		
		msg.put("type","masterStatus");
		
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
			workerInfo.put("connecTime", timeToJson(w.getConnectTime()));
			workerInfo.put("url", w.getSocketURL());
			
			workers.put(w.hashCode(), workerInfo);
		}
		
		if (currentJob != null) {
			// Job information
			Map<Object, Object> jobMap = Message.getJSONMapForJob(currentJob);
			jobMap.put("startTime", timeToJson(currentJob.getStartTime()));
			jobMap.put("finished", currentJob.isFinished());
			payload.put(Message.FIELD_JOB_MAP, jobMap);
		
			// Job progress
			SplitStore ss = currentJob.getSplitInformation();
			PartitionStore ps = currentJob.getPartitionInformation();
			
			// Splits
			Map<Object, Object> splitMap = new HashMap<Object, Object>();
			payload.put("splits", splitMap);
			
			
			Map<Object, Object> doneSplits = new HashMap<Object, Object>();
			splitMap.put("done", doneSplits);
			
			Map<Object, Object> queuedSplits = new HashMap<Object, Object>();
			splitMap.put("queued", queuedSplits);
			
			for (int i = 0; i < currentJob.getSplits(); i++) {
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
			

			// Partitions
			Map<Object, Object> partitionMap = new HashMap<Object, Object>();
			payload.put("partitions", partitionMap);
			
			// done partitions
			partitionMap.put("done", ps.getDonePartitionRanges());
			
			Map<Object, Object> queuedPartitions = new HashMap<Object, Object>();
			partitionMap.put("queued", queuedPartitions);
			
			for (int i = 0; i < currentJob.getPartitions(); i++) {
				Partition partition = new Partition(i);
				
				Set<Worker> tmp;
				tmp = ps.getAllQueuedWorkers(partition);
				if (!tmp.isEmpty()) {
					Set<Integer> who = new HashSet<Integer>();
					for (Worker w : tmp) { who.add(w.hashCode()); }
					queuedPartitions.put(i, who);
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
		//TODO: return (int)(millis/1000); <- switch to ints if longs are too wide for JS
	}
}
