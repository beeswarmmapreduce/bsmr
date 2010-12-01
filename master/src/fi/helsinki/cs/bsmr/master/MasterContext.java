package fi.helsinki.cs.bsmr.master;

import java.util.List;
import java.util.Set;


import fi.helsinki.cs.bsmr.master.console.Console;

public interface MasterContext
{
	// Actual functionality
	public boolean acknowledgeWork(Worker worker, Message msg);
	public Message selectTaskForWorker(Worker worker, Message msg);
	
	// Worker functionality
	public Worker getWorkerByURL(String url);
	public Worker createWorker(String remoteAddr);
	public void   addWorker(Worker worker) throws WorkerInIllegalStateException;
	public void   removeWorker(Worker worker) throws WorkerInIllegalStateException;
	
	public void   setWorkerURL(Worker worker, String socketURL);
	public String getWorkerURL(Worker worker);	
	public Set<Worker> getWorkers();
	
	// Job related functionality
	public Job     createJob(int splits, int partitions, int heartbeatTimeout, int acknowledgeTimeout);
	public Job     getJobById(int jobId);
	public void    queueJob(Job j) throws JobAlreadyRunningException;
	public boolean startNextJob() throws JobAlreadyRunningException;
	
	public List<Job> getJobQueue();
	public List<Job> getJobHistory();
	public Job       getActiveJob();
	
	public boolean isJobActive();
	
	// Console
	public void addConsole(Console c);
	public void removeConsole(Console c);
	public Set<Console> getConsoles();
	
	
}
