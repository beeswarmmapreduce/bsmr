package fi.helsinki.cs.bsmr.master;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class WorkerStore
{
	protected Set<Worker> workers;
	
	// Two-way map, accessed through setWorkerURL() and getWorkerURL(), removeWorker()
	private Map<String, Worker> workerForURL;
	private Map<Worker, String> URLForWorker;
	
	
	public WorkerStore()
	{
		workers      = new HashSet<Worker>();
		workerForURL = new HashMap<String, Worker>();
		URLForWorker = new HashMap<Worker, String>();
	}

	public Worker getWorkerByURL(String url)
	{
		if (url == null) {
			return null;
		}
 
		return workerForURL.get(url);
	}

	public void setWorkerURL(Worker worker, String socketURL)
	{
		synchronized (this) {
			
			// Remove the old URL (if there is one)
			if (URLForWorker.keySet().contains(worker)) {
				String oldURL = URLForWorker.remove(worker);
				if (oldURL != null) {
					workerForURL.remove(oldURL);
				}
			}

			workerForURL.put(socketURL, worker);
			URLForWorker.put(worker, socketURL);
		}
	}
	

	public String getWorkerURL(Worker worker) 
	{
		return URLForWorker.get(worker);
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
			
			String URL = URLForWorker.remove(worker);
			if (URL != null) {
				workerForURL.remove(URL);
			}
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
	
	public abstract boolean acknowledgeWork(Worker worker, Message msg);
	public abstract Message selectTaskForWorker(Worker worker, Message msg);
	public abstract boolean isActive();

	
	
}
