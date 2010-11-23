package fi.helsinki.cs.bsmr.master;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public abstract class WorkerStore
{
	protected Set<Worker> workers;
	protected Map<String, Worker> workerURLs;
	
	
	public WorkerStore()
	{
		workers    = new HashSet<Worker>();
		workerURLs = new HashMap<String, Worker>();
	}

	public Worker getWorkerByURL(String url)
	{
		if (url == null) {
			return null;
		}
		// TODO: this requires an exact string match... 
		return workerURLs.get(url);
	}

	public void setWorkerURL(Worker worker, String socketURL)
	{
		synchronized (this) {
			// Remove old URL, TODO: this is inefficient!
			// Execute remove() without checking contains() because that would be
			// even slower!
			Collection<Worker> values = workerURLs.values();
			values.remove(worker);

			workerURLs.put(socketURL, worker);
		}
	}
	
	public Worker createWorker(String remoteAddr)
	{
		return new Worker(this, remoteAddr);
	}
	

	public void removeWorker(Worker worker) throws WorkerInIllegalStateException
	{
		boolean removed;
		
		synchronized (this) 
		{			
			removed = workers.remove(worker); 
		}
		
		if (!removed) {
			throw new WorkerInIllegalStateException("removeWorker() worker "+worker+" was not registered");
		}
	}
	
	public void addWorker(Worker worker) throws WorkerInIllegalStateException
	{
		synchronized (this) 
		{
			if (workers.contains(worker)) {
				throw new WorkerInIllegalStateException("addWorker() worker "+worker+" already exists");
			}
			
			workers.add(worker);
		}
		
	}
	
	public abstract Message executeWorkerMessage(Worker w, Message msg);
	public abstract boolean isActive();

	public String getWorkerURL(Worker worker) 
	{
		for (Entry<String, Worker> entry : workerURLs.entrySet()) {
			if (entry.getValue() == worker) {
				return entry.getKey();
			}
		}
		return null;
	}
	
}
