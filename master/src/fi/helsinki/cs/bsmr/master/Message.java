package fi.helsinki.cs.bsmr.master;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.eclipse.jetty.util.ajax.JSON;


public class Message 
{
	private static final Logger logger = Util.getLoggerForClass(Message.class);
	
	public enum Type {
		DO, ACK, HB
	}
	
	public enum Action {
		socket, mapTask, reduceTask, reduceSplit, idle	
	}
	
	public static final String FIELD_ACTION         = "action";
	
	public static final String FIELD_PAYLOAD        = "payload";
	public static final String FIELD_TYPE           = "type";
	public static final String FIELD_JOBID          = "jobId";
	public static final String FIELD_JOB_MAP        = "job";
	
	public static final String FIELD_MAPSTATUS      = "mapStatus";
	public static final String FIELD_REDUCESTATUS   = "reduceStatus";
	public static final String FIELD_UNREACHABLE    = "unreachable";
	
	public static final String FIELD_NUM_PARTITIONS = "R";
	public static final String FIELD_NUM_SPLITS     = "M";
	
	public static final String FIELD_SPLITID        = "splitId";
	public static final String FIELD_PARTITIONID    = "partitionId";
	public static final String FIELD_REDUCE_LOCATION = "location";
	
	public static final String FIELD_SOCKET_PROTOCOL = "protocol";
	public static final String FIELD_SOCKET_PORT     = "port";
	public static final String FIELD_SOCKET_RESOURCE = "resource";
	
	
	public static final String FIELD_CODE = "code";
	
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
	
	private Message(Map<Object, Object> d, MasterContext master, String remoteAddr) throws IllegalMessageException
	{
		Map<?, ?> payload = (Map<?, ?>) d.get(FIELD_PAYLOAD);
		if (payload == null) {
			throw new IllegalMessageException("No payload");
		}
		
		String typeField = (String)d.get(FIELD_TYPE);
		try {
			this.type = Type.valueOf(typeField);
		} catch(NullPointerException npe) {
			throw new IllegalMessageException("No type field");
		} catch(IllegalArgumentException iae) {
			throw new IllegalMessageException("Message type '"+typeField+"' not recognized"); 
		}
		
		String actionField = (String)payload.get(FIELD_ACTION);
		try {
			this.action = Action.valueOf(actionField);
		} catch(NullPointerException npe) {
			throw new IllegalMessageException("No action field in payload");
		} catch(IllegalArgumentException iae) {
			throw new IllegalMessageException("Message action '"+actionField+"' not recognized");
		}
		
		if (payload.containsKey(FIELD_JOBID)) {
			Long tmp = (Long)payload.get(FIELD_JOBID);
			if (tmp != null) {
				this.job = master.getJobById( tmp.intValue() );
			}
		}
		
	
		this.socketUrl = constructURL((String)payload.get(FIELD_SOCKET_PROTOCOL),
				                      remoteAddr,payload.get(FIELD_SOCKET_PORT),
				                      (String)payload.get(FIELD_SOCKET_RESOURCE));
		this.mapStatus    = createMapStatus((Map<?, ?>)payload.get(FIELD_MAPSTATUS));
		this.reduceStatus = createReduceStatus((Map<?, ?>)payload.get(FIELD_REDUCESTATUS));
				
		this.unreachableWorkers = parseWorkers( payload.get(FIELD_UNREACHABLE), master);
	}
	
	public static String constructURL(String protocol, String remoteAddr, Object port, String resource)
	{
		if (protocol == null || port == null || resource == null) return null;
		
		StringBuffer ret = new StringBuffer();
		ret.append(protocol);
		ret.append("://");
		ret.append(remoteAddr);
		ret.append(":");
		ret.append(port);
		if (resource.length() > 0 && resource.charAt(0) != '/') {
			ret.append('/');
		}
		ret.append(resource);
		
		return ret.toString();
	}
	
	@SuppressWarnings("unchecked")
	private static Set<Worker> parseWorkers(Object workersAsUrls, MasterContext workers)
	{
		if (workersAsUrls == null) return null;
		
		Collection<Object> set;
		if (workersAsUrls instanceof Collection) {
			set = (Collection<Object>)workersAsUrls;
		} else if (workersAsUrls instanceof Map) {
			logger.fine("Parsing workers from message using a map?? Using the keys");
			set = ((Map)workersAsUrls).keySet();
		} else {
			try {
				Object []tmp = (Object[])workersAsUrls;
				set = new ArrayList<Object>();
				for (Object o : tmp) {
					set.add(o);
				}
			} catch(ClassCastException cce) {
				if (workersAsUrls != null) {
					logger.severe("Unable to use workersAsUrls parameter: "+workersAsUrls);
				}
				return Collections.emptySet();
			}
			
			
		}
		
		Set<Worker> ret = new HashSet<Worker>();
		
		for (Object o: set) {
			if (!(o instanceof String)) {
				logger.severe("payload contains non-string worker URLs");
				continue;
			}
			String url = (String)o;
			Worker w = workers.getWorkerByURL(url);
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
	
	public MapStatus getMapStatus()
	{
		return mapStatus;
	}
	
	public ReduceStatus getReduceStatus()
	{
		return reduceStatus;
	}
	
	/**
	 * @param msg Message as string
	 * @return Parsed Message
	 */
	@SuppressWarnings("unchecked")
	public static Message parseMessage(String msg, MasterContext master, String remoteAddr) throws IllegalMessageException
	{
		Map<Object, Object> tmp = (Map<Object, Object>)JSON.parse(msg);
		try {
			return new Message(tmp, master, remoteAddr);
		} catch(IllegalMessageException ime) {
			ime.setProblematicMessage(msg);
			throw ime;
		}
	}


	/**
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
			Map<Object, Object> jobMap = getJSONMapForJob(job);
			
			payload.put(FIELD_JOB_MAP, jobMap);
		}
		
		return JSON.toString(data);
	}

	public static Map<Object, Object> getJSONMapForJob(Job job)
	{
		Map<Object, Object> jobMap = new HashMap<Object, Object>();
		jobMap.put(FIELD_JOBID, job.getJobId());
		jobMap.put(FIELD_NUM_SPLITS, job.getSplits());
		jobMap.put(FIELD_NUM_PARTITIONS, job.getPartitions());
		jobMap.put(FIELD_CODE, job.getCode());
		return jobMap;
	}
	
	public String toString()
	{
		return encodeMessage();
	}
	
	public static Message pauseMessage()
	{
		return new Message(Type.DO, Action.idle);
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
	
	public static Message findSplitAtMessage(Partition p, Split s, Job j, Set<Worker> unreachableWorkers)
	{
		Message ret = new Message(Type.DO, Action.reduceSplit, j);
		Set<Worker> hasSplit = new HashSet<Worker>();
		
		for (Worker w : j.getSplitInformation().whoHasSplit(s)) {
			
			if (unreachableWorkers.contains(w)) continue;
			
			hasSplit.add(w);
		}
		
		ret.reduceStatus = ret.new ReduceStatus(p, s, hasSplit);
		return ret;
	}

	public Set<Worker> getUnareachableWorkers() 
	{
		if (unreachableWorkers == null) return Collections.emptySet();
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
		
		Long tmp = (Long)map.get(FIELD_SPLITID);
		if (tmp == null) return null;
		
		Split s = new Split(tmp.intValue());
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
			if (split != null) ret.put(FIELD_SPLITID, split.getId());
			
			if (location != null) {
				Set<String> tmp = new HashSet<String>();
				for (Worker w : location) {
					tmp.add(w.getSocketURL());
				}	
				ret.put(FIELD_REDUCE_LOCATION, tmp);	
			}
			
			return ret;
		}
		
		Partition partition;
		Split split;
		Set<Worker> location;
	}
	
	public ReduceStatus createReduceStatus(Map<?, ?> map) 
	{
		if (map == null) return null;
		
		Long tmp1 = (Long)map.get(FIELD_SPLITID);
		Long tmp2 = (Long)map.get(FIELD_PARTITIONID);
		
		Split s = null;
		if (tmp1 != null) s = new Split(tmp1.intValue());
		
		Partition p = null;
		if (tmp2 != null) p = new Partition(tmp2.intValue());

		// TODO: no locations in ACK messsages
		
		return new ReduceStatus(p, s, null);
	}
}
