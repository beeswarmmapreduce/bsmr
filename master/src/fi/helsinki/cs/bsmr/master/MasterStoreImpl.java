package fi.helsinki.cs.bsmr.master;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import fi.helsinki.cs.bsmr.master.console.Console;

public abstract class MasterStoreImpl implements MasterContext 
{
	private static Logger logger = Util.getLoggerForClass(MasterStoreImpl.class);
	
	private Set<Worker> workers;
	
	// Two-way map, accessed through setWorkerURL() and getWorkerURL(), removeWorker()
	private Map<String, Worker> workerForURL;
	private Map<Worker, String> URLForWorker;
	
	// Job ID -> Job
	private Map<Integer, Job> jobMap; 
	
	private List<Job> jobQueue;
	private List<Job> jobHistory;
	private Job activeJob;
	
	// Console
	private Set<Console> consoles;
	
	public MasterStoreImpl()
	{
		workers      = new HashSet<Worker>();
		workerForURL = new HashMap<String, Worker>();
		URLForWorker = new HashMap<Worker, String>();
		
		activeJob  = null;
		jobQueue   = new LinkedList<Job>();
		jobHistory = new LinkedList<Job>();
		
		jobMap     = new HashMap<Integer, Job>();
		
		consoles   = new HashSet<Console>();
	}

	public Worker getWorkerByURL(String url)
	{
		if (url == null) {
			return null;
		}
 
		return workerForURL.get(url);
	}
	
	public Set<Worker> getWorkers()
	{
		return Collections.unmodifiableSet(workers);
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

	// Job functionality
	public synchronized boolean startNextJob() throws JobAlreadyRunningException
	{
		if (activeJob != null && !activeJob.isFinished()) {
			logger.severe("Tried to start next job, but we have a non-finished active job!");
			throw new JobAlreadyRunningException("Tried to start next job, but we have a non-finished active job!");
		}
		
		if (jobQueue.isEmpty()) {
			logger.fine("Tried to start next job, but no jobs in queue");
			return false;
		}
		
		if (activeJob != null) {
			jobHistory.add(activeJob);
		}
		
		activeJob = jobQueue.remove(0);
		activeJob.startJob();
		
		Set<Worker> badWorkers = null;
		
		Message dummyStatus = Message.pauseMessage();
		
		for (Worker w : getWorkers()) {
			// Skip inactive workers because they might be dead and communicating with them would slow down operations
			// if IO is async, this is not necessary! But.. Looking at Jetty 8.0.0.M2 sources, it seems (although
			// it is difficult to verify) that sendMessage() is in fact synchronous.
			if (!w.isAvailable(activeJob)) continue;
			
			Message msg = selectTaskForWorker(w, dummyStatus);
			
			try {
				w.sendMessage(msg);
			} catch(IOException ie) {
				logger.log(Level.SEVERE, "Could not send work to worker. Terminating connection.", ie);
				if (badWorkers == null) badWorkers = new HashSet<Worker>();
				
				badWorkers.add(w);
			}
		}

		if (badWorkers != null) {
			for (Worker bad : badWorkers) {
				try {
					removeWorker(bad);
				} catch(WorkerInIllegalStateException wiise) {
					logger.log(Level.SEVERE, "Bad worker could not be removed?.. perhaps it was removed by a callback", wiise);
				} finally {
					bad.disconnect();
				}
			}
		}
		
		return true;
	}

	public synchronized void queueJob(Job j) throws JobAlreadyRunningException
	{
		if (j.isStarted() || j.isFinished()) {
			logger.severe("Tried to add job "+j+" to job queue, but it's in an illegal state (started or finished)");
			throw new JobAlreadyRunningException("Tried to add job "+j+" to job queue, but it's in an illegal state (started or finished)");
		}
		
		jobQueue.add(j);
	}
	
	public List<Job> getJobQueue()
	{
		return Collections.unmodifiableList(jobQueue);
	}

	public List<Job> getJobHistory() 
	{
		return Collections.unmodifiableList(jobHistory);
	}

	public Job getActiveJob()
	{
		return activeJob;
	}
	
	public boolean isJobActive()
	{
		return activeJob != null && !activeJob.isFinished() && activeJob.isStarted();
	}

	public Job createJob(int splits, int partitions, int heartbeatTimeout, int acknowledgeTimeout)
	{
		Job ret;
		
		synchronized (this) {
			// Monotonic for (2^31-1)/100/60/60/24 days (248.5), one new ID per 1/100th of a second
			int thisJob = -1;
			do {
				if (thisJob > 0) { try { Thread.sleep(10); } catch(Exception e) {} }
				thisJob = (int)((System.currentTimeMillis()/100) % Integer.MAX_VALUE);
				
			} while (jobMap.containsKey(thisJob));
			ret = new Job(thisJob, splits, partitions, heartbeatTimeout, acknowledgeTimeout);
			jobMap.put(thisJob, ret); // auto-boxing
		}
		
		logger.info("Created new Job "+ret);
		
		return ret;
	}
	
	public Job getJobById(int jobId)
	{
		return jobMap.get(jobId); // auto-boxing
	}
	
	
	// Console functions
	public void addConsole(Console c)
	{
		synchronized (consoles) {
			consoles.add(c);
		}
	}

	public void removeConsole(Console c)
	{
		synchronized (consoles) {
			consoles.remove(c);
		}	
	}

	public Set<Console> getConsoles()
	{
		return Collections.unmodifiableSet(consoles);
	}

}
