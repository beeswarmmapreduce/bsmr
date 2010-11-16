package fi.helsinki.cs.bsmr.master;


public class Message 
{
	public enum Type {
		DO("DO"),
		ACK("ACK");
		
		private final String type;
		Type(String str) { type = str; }
		
		public String toString() { return type; }
	}
	
	public enum Action {
		UP("up"),
		MAP("map"),
		REDUCETASK("reducetask"),
		REDUCE("reduce"),
		HEARTBEAT("heartbeat"),
		PAUSE("pause");
		
		private final String action;
		Action(String str) { action = str; }
		
		public String toString() { return action; }	
	}
	
	
	private Type type;
	private Action action;
	
	private Job job;
	
	public Message(Type t, Action a, int jobId)
	{
		this.type   = t;
		this.action = a;
		this.job    = Job.getJobById(jobId);
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
	public static Message parseMessage(String msg)
	{
		return new Message(Type.ACK, Action.REDUCE, 1);
	}


	/**
	 * TODO: insert JSON
	 * @return This message encoded into a JSON string
	 */
	public String encodeMessage()
	{
		StringBuffer ret = new StringBuffer();
		ret.append(type+": { action: '"+action+"'");
		if (job != null) {
			ret.append(", jobid: "+job.getJobId());
		}
		ret.append(" }");
		
		return ret.toString();
	}
	
	public String toString()
	{
		return encodeMessage();
	}
}
