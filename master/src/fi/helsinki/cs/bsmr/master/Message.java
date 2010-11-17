package fi.helsinki.cs.bsmr.master;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.jetty.util.ajax.JSON;


public class Message 
{
	public enum Type {
		DO, ACK, HB
	}
	
	public enum Action {
		socket, mapTask, reduceTask, reduceSplit, idle	
	}
	
	private static final String FIELD_ACTION         = "action";
	private static final String FIELD_TYPE           = "type";
	private static final String FIELD_JOBID          = "jobId";
	
	private static final String FIELD_NUM_PARTITIONS = "R";
	private static final String FIELD_NUM_SPLITS     = "M";
	
	private static final String FIELD_SPLITID        = "splitId";
	private static final String FIELD_PARTITIONID    = "partitionId";
	
	private static final String FIELD_SPLITLOCATION  = "splitlocation";
	private static final String FIELD_SPLITLOCATION_SPLITID = "splitId";
	private static final String FIELD_SPLITLOCATION_URLS    = "urls";
	
	private Map<Object, Object> data;
	
	private Job job;
	
	private Message(Type t, Action a)
	{
		this.job    = null;
		
		data = new HashMap<Object, Object>();
		data.put(FIELD_ACTION, a);
		data.put(FIELD_TYPE,   t);
	}
	
	private Message(Type t, Action a, Job j)
	{
		this.job    = j;
		
		data = new HashMap<Object, Object>();
		data.put(FIELD_ACTION, a);
		data.put(FIELD_TYPE,   t);
		
		if (job != null) {
			data.put(FIELD_JOBID,          job.getJobId());
			data.put(FIELD_NUM_PARTITIONS, job.getPartitions());
			data.put(FIELD_NUM_SPLITS,     job.getSplits());
		}
	}
	
	private Message(Map<Object, Object> d)
	{
		this.data = d;
		
		data.put(FIELD_ACTION, Action.valueOf((String)data.get(FIELD_ACTION)));
		data.put(FIELD_TYPE,   Type.valueOf((String)data.get(FIELD_TYPE)));
		
		if (data.containsKey(FIELD_JOBID)) {
			Object o = data.get(FIELD_JOBID);
			
			if (o instanceof Integer) {
				this.job = Job.getJobById((Integer)o);
			} else if (o instanceof String) {
				this.job = Job.getJobById(Integer.parseInt((String)o));
			}
		}
	}
	
	public Type getType()
	{
		return (Type)data.get(FIELD_TYPE);
	}
	
	public Action getAction()
	{
		return (Action)data.get(FIELD_ACTION);
	}
	
	public Job getJob()
	{
		return job;
	}
	
	/**
	 * TODO: insert JSON
	 * @param msg Message as string
	 * @return Parsed Message
	 */
	public static Message parseMessage(String msg)
	{
		Map<Object, Object> tmp = (Map<Object, Object>)JSON.parse(msg);
		return new Message(tmp);
	}


	/**
	 * TODO: insert JSON
	 * @return This message encoded into a JSON string
	 */
	public String encodeMessage()
	{
		return JSON.toString(data);
	}
	
	public String toString()
	{
		return encodeMessage();
	}
	
	
	public static Message pauseMessage()
	{
		return new Message(Type.DO, Action.idle);
	}
	
	public static Message mapThisMessage(Split s, Job j)
	{
		Message ret = new Message(Type.DO, Action.mapTask, j);
		ret.data.put(FIELD_SPLITID, s.getId());
		return ret;
	}
	
	public static Message reduceThatMessage(Partition p, Job j)
	{
		Message ret = new Message(Type.DO, Action.reduceTask, j);
		ret.data.put(FIELD_PARTITIONID, p.getId());
		return ret;
	}
	
	public static Message findSplitAtMessage(Partition p, Split s, Job j)
	{
		Message ret = new Message(Type.DO, Action.reduceSplit, j);
		ret.data.put(FIELD_PARTITIONID, p.getId());
		
		
		
		List<String> urls = new ArrayList<String>();
		
		for (Worker w : j.getSplitInformation().whoHasSplit(s)) {
			urls.add(w.getSocketURL());
		}
		
		Map<String, Object> splitLocation = new HashMap<String, Object>();
		splitLocation.put(FIELD_SPLITLOCATION_SPLITID, s.getId());
		splitLocation.put(FIELD_SPLITLOCATION_URLS, urls);
		
		
		ret.data.put(FIELD_SPLITLOCATION, splitLocation);
		
		return ret;
	}

	public Set<Worker> getUnareachableWorkers() {
		// TODO: create this
		return null;
	}

	public String getSocketURL() {
		// TODO Auto-generated method stub
		return null;
	}

	public Partition getIncompleteReducePartition() {
		// TODO Auto-generated method stub
		return null;
	}
}
