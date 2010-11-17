package fi.helsinki.cs.bsmr.master;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
	
	private static final String FIELD_PAYLOAD        = "payload";
	private static final String FIELD_TYPE           = "type";
	private static final String FIELD_JOBID          = "jobId";
	private static final String FIELD_JOB_MAP        = "job";
	private static final String FIELD_URL            = "url";
	
	private static final String FIELD_MAPSTATUS      = "mapStatus";
	private static final String FIELD_REDUCESTATUS   = "reduceStatus";
	private static final String FIELD_UNREACHABLE    = "unreachable";
	
	private static final String FIELD_NUM_PARTITIONS = "R";
	private static final String FIELD_NUM_SPLITS     = "M";
	
	private static final String FIELD_SPLITID        = "splitId";
	private static final String FIELD_PARTITIONID    = "partitionId";
	private static final String FIELD_REDUCE_LOCATION = "location";
	
	private static final String FIELD_CODE = "code";
	
	private Type type;
	private Action action;
	
	private Job job;
	
	private String socketUrl;
	private MapStatus mapStatus;
	private ReduceStatus reduceStatus;
	private Set<Worker> unreachableWorkers;
	
	
	private Message(Type t, Action a)
	{
		this.type = t;
		this.action = a;
		
		this.job                = null;
		this.socketUrl          = null;
		this.mapStatus          = null;
		this.reduceStatus       = null;
		this.unreachableWorkers = null;
	}
	
	private Message(Type t, Action a, Job j)
	{
		this.type   = t;
		this.action = a;
		this.job    = j;
		
		this.socketUrl          = null;
		this.mapStatus          = null;
		this.reduceStatus       = null;
		this.unreachableWorkers = null;
	}
	
	private Message(Map<Object, Object> d, Master master)
	{
		Map<?, ?> payload = (Map<?, ?>) d.get(FIELD_PAYLOAD);
		
		this.type = Type.valueOf((String)d.get(FIELD_TYPE));
		this.action = Action.valueOf((String)payload.get(FIELD_ACTION));
		
		if (payload.containsKey(FIELD_JOBID)) {
			Object o = payload.get(FIELD_JOBID);
			
			if (o instanceof Integer) {
				this.job = Job.getJobById((Integer)o);
			} else if (o instanceof String) {
				this.job = Job.getJobById(Integer.parseInt((String)o));
			}
		}
		
		this.socketUrl = (String)payload.get(FIELD_URL);
		this.mapStatus    = createMapStatus((Map<?, ?>)payload.get(FIELD_MAPSTATUS));
		this.reduceStatus = createReduceStatus((Map<?, ?>)payload.get(FIELD_REDUCESTATUS));
				
		this.unreachableWorkers = parseWorkers( (Collection<String>)payload.get(FIELD_UNREACHABLE), master);
	}
	
	private static Set<Worker> parseWorkers(Collection<String> workersAsUrls, Master master)
	{
		if (workersAsUrls == null) return null;
		
		Set<Worker> ret = new HashSet<Worker>();
		
		for (String url : workersAsUrls) {
			Worker w = master.getWorkerByURL(url);
			if (w != null) {
				ret.add(w);
			}
		}
		
		return ret;
	}

	public Type getType()
	{
		return type;
	}
	
	public Action getAction()
	{
		return action;
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
	public static Message parseMessage(String msg, Master master)
	{
		Map<Object, Object> tmp = (Map<Object, Object>)JSON.parse(msg);
		return new Message(tmp, master);
	}


	/**
	 * TODO: insert JSON
	 * @return This message encoded into a JSON string
	 */
	public String encodeMessage()
	{
		Map<Object, Object> data = new HashMap<Object, Object>();
		Map<Object, Object> payload = new HashMap<Object, Object>();
		
		data.put(FIELD_TYPE, type.toString());
		data.put(FIELD_PAYLOAD, payload);
		
		
		payload.put(FIELD_ACTION, action.toString());
		if (reduceStatus != null) {
			payload.put(FIELD_REDUCESTATUS, reduceStatus.asMap());
		}
		if (mapStatus != null) {
			payload.put(FIELD_MAPSTATUS, mapStatus.asMap());
		}
		
		
		if (job != null) {
			Map<Object, Object> jobMap = new HashMap<Object, Object>();
			jobMap.put(FIELD_JOBID, job.getJobId());
			jobMap.put(FIELD_NUM_SPLITS, job.getSplits());
			jobMap.put(FIELD_NUM_PARTITIONS, job.getPartitions());
			
			payload.put(FIELD_JOB_MAP, jobMap);
		}
		
		payload.put(FIELD_CODE, job.getCode());
		
		return JSON.toString(data);
	}
	
	public String toString()
	{
		return encodeMessage();
	}
	
	
	public static Message pauseMessage(Job j)
	{
		return new Message(Type.DO, Action.idle, j);
	}
	
	public static Message mapThisMessage(Split s, Job j)
	{
		Message ret = new Message(Type.DO, Action.mapTask, j);
		ret.mapStatus = ret.new MapStatus(s);
		return ret;
	}
	
	public static Message reduceThatMessage(Partition p, Job j)
	{
		Message ret = new Message(Type.DO, Action.reduceTask, j);
		ret.reduceStatus = ret.new ReduceStatus(p, null, null);
		return ret;
	}
	
	public static Message findSplitAtMessage(Partition p, Split s, Job j)
	{
		Message ret = new Message(Type.DO, Action.reduceSplit, j);
		ret.reduceStatus = ret.new ReduceStatus(p, s, j.getSplitInformation().whoHasSplit(s));
		return ret;
	}

	public Set<Worker> getUnareachableWorkers() 
	{
		return unreachableWorkers;
	}

	public String getSocketURL() 
	{
		return socketUrl;
	}

	public Partition getIncompleteReducePartition() 
	{
		return reduceStatus.partition;
	}
	
	public class MapStatus
	{
		public MapStatus(Split s)
		{
			this.split = s;
		}
		
		public Map<Object, Object> asMap()
		{
			Map<Object, Object> ret = new HashMap<Object, Object>();
			ret.put(FIELD_SPLITID, split.getId());
			return ret;
		}
		
		Split split;
	}
	
	public MapStatus createMapStatus(Map<?,?> map) 
	{
		if (map == null) return null;
		
		String tmp = (String)map.get(FIELD_SPLITID);
		if (tmp == null) return null;
		
		Split s = new Split(Integer.parseInt(tmp));
		return new MapStatus(s);
	}
	
	public class ReduceStatus
	{
		public ReduceStatus(Partition p, Split s, Set<Worker> l)
		{
			this.partition = p;
			this.split = s;
			this.location = l;
		}
		
		
		public Map<Object, Object> asMap()
		{
			Map<Object, Object> ret = new HashMap<Object, Object>();
			
			ret.put(FIELD_PARTITIONID, partition.getId());
			ret.put(FIELD_SPLITID, split.getId());
			ret.put(FIELD_REDUCE_LOCATION, location);
			
			return ret;
		}
		
		Partition partition;
		Split split;
		Set<Worker> location;
	}
	
	public ReduceStatus createReduceStatus(Map<?, ?> map) 
	{
		if (map == null) return null;
		
		String tmp1 = (String)map.get(FIELD_SPLITID);
		String tmp2 = (String)map.get(FIELD_PARTITIONID);
		
		Split s = null;
		if (tmp1 != null) s = new Split(Integer.parseInt(tmp1));
		
		Partition p = null;
		if (tmp2 != null) p = new Partition(Integer.parseInt(tmp2));

		// TODO: no locations in ACK messsages
		
		return new ReduceStatus(p, s, null);
	}
}
