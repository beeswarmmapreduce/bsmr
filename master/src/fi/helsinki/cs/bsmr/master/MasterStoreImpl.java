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
import fi.helsinki.cs.bsmr.master.console.ConsoleInformation;

/**
 * Implementation of object storage parts of the MasterContext interface.
 *  
 * @author stsavola
 *
 */
public abstract class MasterStoreImpl implements MasterContext 
{
	private static Logger logger = Util.getLoggerForClass(MasterStoreImpl.class);
	
	private Set<Worker> workers;
	
	// Two-way map, accessed through setWorkerURL() and getWorkerURL(), removeWorker()
	private Map<String, Worker> workerForURL;
	private Map<Worker, String> URLForWorker;
	
	// Job ID -> Job
	private Map<Integer, Job> jobMap;
	
	// A copy of the real jobMap used for transient read operations. This allows
	// for concurrent parsing of IDs to Jobs.
	private Map<Integer, Job> readOnlyJobMap; 
	
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
		readOnlyJobMap   = new HashMap<Integer, Job>();
		
		consoles   = new HashSet<Console>();
	}

	// **************************** Worker functionality
	
	@Override
	public Worker getWorkerByURL(String url)
	{
		if (url == null) {
			return null;
		}
 
		return workerForURL.get(url);
	}
	
	@Override
	public Set<Worker> getWorkers()
	{
		return Collections.unmodifiableSet(workers);
	}

	@Override
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
	
	@Override
	public String getWorkerURL(Worker worker) 
	{
		return URLForWorker.get(worker);
	}
	
	@Override
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
			
			if (activeJob != null) {
				activeJob.getSplitInformation().removeWorkerInformation(worker);
				activeJob.getPartitionInformation().removeWorkerInformation(worker);
			}
		}
		
		if (!removed) {
			throw new WorkerInIllegalStateException("removeWorker() worker "+worker+" was not registered");
		}
	}
	
	@Override
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

	// **************************** Job functionality
	
	@Override
	public synchronized boolean startNextJob() throws JobAlreadyRunningException
	{
		if (activeJob != null && activeJob.getState() == Job.State.RUNNING) {
			logger.severe("Tried to start next job, but we have a running active job!");
			throw new JobAlreadyRunningException("Tried to start next job, but we have a running active job!");
		}
		
		if (jobQueue.isEmpty()) {
			logger.fine("Tried to start next job, but no jobs in queue");
			return false;
		}
		
		if (activeJob != null) {
			jobHistory.add(activeJob);
			activeJob = null;
		}
		
		activeJob = jobQueue.remove(0);
		activeJob.startJob();
		
		// this is needed as selectTaskForWorker depends on a request message
		Message dummyMsg = Message.pauseMessage();
		
		for (Worker w : getWorkers()) {
			// TODO: update this comment to the year 2010. 
			// Skip inactive workers because they might be dead and communicating with them would slow down operations
			// if IO is async, this is not necessary! But.. Looking at Jetty 8.0.0.M2 sources, it seems (although
			// it is difficult to verify) that sendMessage() is in fact synchronous.
			if (!w.isAvailable(activeJob)) continue;
			
			Message msg = selectTaskForWorker(w, dummyMsg);
			
			w.sendAsyncMessage(msg, 1000); // TODO: use a static 1s ramp up for now. Add it to Job parameters if deemed necessary
			
		}
		
		return true;
	}

	@Override
	public synchronized void removeJob(Job toBeRemoved)
	{
		logger.fine("Removing ID -> Job mapping");
		// Remove id -> Job mapping
		jobMap.remove(toBeRemoved.getJobId());
		
		if (jobQueue.contains(toBeRemoved)) {
			logger.fine("Removing job from Job queue");
			jobQueue.remove(toBeRemoved);
		}
		
		if (jobHistory.contains(toBeRemoved)) {
			logger.fine("Removing job from Job history");
			jobHistory.remove(toBeRemoved);
		}
		
		if (activeJob != null &&
			activeJob.equals(toBeRemoved)) {
			logger.fine("Removing current active job");
			if (activeJob.getState() != Job.State.FINISHED) {
				logger.fine(" .. mark the job as finished");
				activeJob.finishJob();
			}
			
			// TODO: do not add removed job into the job history
			//logger.fine(" .. move job into Job history");
			//jobHistory.add(activeJob);
			activeJob = null;
			
			logger.fine(" .. attempting to start the next job");
			try {
				startNextJob();
			} catch(JobAlreadyRunningException jare) {
				logger.log(Level.SEVERE, "We just stopped the previous job, but startNextJob() tells us there is a job running?!", jare);
			}
				
		}
		
		copyJobMap();
	}
	
	@Override
	public synchronized void queueJob(Job j) throws JobAlreadyRunningException
	{
		if (j.getState() != Job.State.NEW) {
			logger.severe("Tried to add job "+j+" to job queue, but it's in an illegal state (state != NEW)");
			throw new JobAlreadyRunningException("Tried to add job "+j+" to job queue, but it's in an illegal state (state != NEW)");
		}
		
		jobQueue.add(j);
	}
	
	@Override
	public List<Job> getJobQueue()
	{
		return Collections.unmodifiableList(jobQueue);
	}

	@Override
	public List<Job> getJobHistory() 
	{
		return Collections.unmodifiableList(jobHistory);
	}

	@Override
	public Job getActiveJob()
	{
		return activeJob;
	}
	
	@Override
	public boolean isJobRunning()
	{
		return activeJob != null && activeJob.getState() == Job.State.RUNNING;
	}

	@Override
	public Job createJob(int splits, int partitions, int heartbeatTimeout, int acknowledgeTimeout, Object code)
	{
		Job ret;
		
		synchronized (this) {
			// Monotonic for (2^31-1)/100/60/60/24 days (248.5), one new ID per 1/100th of a second
			int thisJob = -1;
			do {
				if (thisJob > 0) { try { Thread.sleep(10); } catch(Exception e) {} }
				thisJob = (int)((System.currentTimeMillis()/100) % Integer.MAX_VALUE);
				
			} while (jobMap.containsKey(thisJob));
			ret = new Job(thisJob, splits, partitions, heartbeatTimeout, acknowledgeTimeout, code);
			jobMap.put(thisJob, ret); // auto-boxing
			
			copyJobMap();
		}
		
		logger.info("Created new Job "+ret);
		
		
		
		return ret;
	}
	
	private void copyJobMap()
	{
		Map<Integer, Job> tmp = new HashMap<Integer, Job>(jobMap);
		readOnlyJobMap = tmp;
	}
	
	@Override
	public Job getJobById(int jobId)
	{
		return readOnlyJobMap.get(jobId); // auto-boxing
	}
	
	// **************************** Console functionality
	
	@Override
	public void addConsole(Console c)
	{
		synchronized (consoles) {
			consoles.add(c);
		}
	}

	@Override
	public void removeConsole(Console c)
	{
		synchronized (consoles) {
			consoles.remove(c);
		}	
	}

	@Override
	public Set<Console> getConsoles()
	{
		return Collections.unmodifiableSet(consoles);
	}
	
	@Override
	public ConsoleInformation getConsoleInformation()
	{
		ConsoleInformation ci;
		
		synchronized (this) {
			ci = new ConsoleInformation(this);
		}

		return ci;
	}

}
