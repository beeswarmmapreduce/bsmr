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

import java.util.List;
import java.util.Set;


import fi.helsinki.cs.bsmr.master.console.Console;
import fi.helsinki.cs.bsmr.master.console.ConsoleInformation;

/**
 * Defines methods needed to implement a BSMR master
 * 
 * @author stsavola
 *
 */
public interface MasterContext
{
	// Actual functionality
	
	/**
	 * Acknowledge the work specified in the message.
	 * 
	 * @param worker The worker who is acknowledging work
	 * @param msg The acknowledgment message 
	 */
	public boolean acknowledgeWork(Worker worker, Message msg);
	
	/**
	 * Select next task for the worker.
	 * 
	 * @param worker The worker who we are selecting work for
	 * @param msg The message sent by the worker (required for the reduceSplit reply)
	 * @return The message to be sent to the worker
	 */
	public Message selectTaskForWorker(Worker worker, Message msg);
	
	// Worker functionality

	/**
	 * Add a new worker. This should be called only from the worker onConnect callback.
	 * 
	 * @param worker The worker to add
	 * @throws WorkerInIllegalStateException The exception is thrown if the worker was already registered. 
	 * @see Worker#onConnect
	 */
	public void   addWorker(Worker worker) throws WorkerInIllegalStateException;
	
	/**
	 * Remove all information relating to this worker. This should be called only from the
	 * worker onDisconnect callback.
	 * 
	 * @param worker The worker to remove
	 * @throws WorkerInIllegalStateException The exception is thrown if the worker is not registered.
	 * @see Worker#onDisconnect()
	 */
	public void   removeWorker(Worker worker) throws WorkerInIllegalStateException;
	
	/**
	 * Set a URL for the worker.
	 * 
	 * @param worker The worker whose URL we are saving
	 * @param socketURL The URL for the worker
	 */
	public void   setWorkerURL(Worker worker, String socketURL);
	
	/**
	 * Return the URL for the worker. This information is stored in the master because we need
	 * a method of connecting an URL to a worker.
	 * 
	 * @param worker The worker we want an URL for
	 * @return The URL for the worker or null if the worker has not sent a socket announcement.
	 * @see MasterContext#getWorkerByURL(String)
	 */
	public String getWorkerURL(Worker worker);
	
	/**
	 * Find out which worker has this URL.
	 * 
	 * @param url The URL for which we want a Worker for
	 * @return The worker who has sent a socket announcement resulting in said URL.
	 */
	public Worker getWorkerByURL(String url);
	
	/**
	 * @return A set of all workers who are registered to this master.
	 */
	public Set<Worker> getWorkers();
	
	// Job related functionality
	
	/**
	 * Create a new job. The Master is responsible for creating a unique id for the job.
	 * @see Job#Job(int, int, int, int, int, Object, Object)
	 * @return the new Job.
	 */
	public Job     createJob(int splits, int partitions, int heartbeatTimeout, int acknowledgeTimeout, Object code, Object inputRef);
	
	/**
	 * Retrieve the job object for the unique id.
	 * 
	 * @param jobId The job id we are interested in.
	 * @return The Job object or null if there is no job for that id.
	 */
	public Job     getJobById(int jobId);
	
	/**
	 * Adds a job to the end of the job queue. 
	 * 
	 * @param j The job to be added to the queue.
	 * @throws JobAlreadyRunningException This exception is thrown if the job j is already running or it is already finished.
	 */
	public void    queueJob(Job j) throws JobAlreadyRunningException;
	
	/**
	 * If there is no current running job, the next job will be picked from the job queue and that will
	 * be started. Notifications will be sent to all workers.
	 * 
	 * @return True if a new job was started, false if there were no jobs queued.
	 * @throws JobAlreadyRunningException This exception is thrown if there was an running job when this was called.
	 */
	public boolean startNextJob() throws JobAlreadyRunningException;
	
	/**
	 * Remove the job. If this job is running, it will be stopped and the next job will be started.
	 * All information about the job will be removed (regardless whether the job was queued, active
	 * or finished).
	 * 
	 * @param toBeRemoved The job that will be removed.
	 */
	public void    removeJob(Job toBeRemoved);
	
	/**
	 * Get a list of jobs that have been queued for running.
	 * 
	 * @return The current job queue.
	 */
	public List<Job> getJobQueue();
	
	/**
	 * Get a list of jobs which have been finished but not removed from the master. Note that
	 * if there is no currently running job, the active job for the master might be a finished job.
	 * This job is not featured in this list.
	 * 
	 * @return A list of finished jobs.
	 * @see MasterContext#getActiveJob()
	 */
	public List<Job> getJobHistory();
	
	/**
	 * Returns the current active job. Note that if there are no new jobs to start when a job
	 * finishes, that job will stay as the active job until a new job is started. Thus the
	 * correct way to test if there is an active job is to call isJobActive() instead of testing
	 * whether this function returns null or not.
	 * 
	 * @return The current "active" job. See description for details.
	 */
	public Job       getActiveJob();
	
	/**
	 * @return Whether there is a current active job
	 */
	public boolean isJobRunning();
	
	// Console
	/**
	 * Add a console to the master.
	 */
	public void addConsole(Console c);
	
	/**
	 * Remove a console from the master.
	 * @param c The console to be removed
	 */
	public void removeConsole(Console c);
	
	/**
	 * @return Get a full list of Consoles attached to this master
	 */
	public Set<Console> getConsoles();

	/**
	 * @return A collection of information needed for status messages 
	 */
	public ConsoleInformation getConsoleInformation();
	
}
