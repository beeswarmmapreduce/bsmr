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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Keeps the state of buckets for a single job.
 * 
 * @author stsavola
 *
 */
public class BucketStore implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	private List<Set<Worker>> bucketsQueued;
	private List<Set<Integer>> bucketsDone2;

	private boolean allBucketsDone; 
	
	private List<Bucket> workQueue;
	private int previousIndexOfWorkQueue;
	
	/**
	 * Create a new BucketStore for the job. The Job is only used for initializing the
	 * data structures and is not needed to assess the state of workers.
	 * 
	 * @param job The job this BucketStore is for
	 */
	public BucketStore(Job job)
	{
		this.bucketsQueued = new ArrayList<Set<Worker>>();
		this.bucketsDone2 = new ArrayList<Set<Integer>>();
		this.workQueue = new LinkedList<Bucket>();
		this.previousIndexOfWorkQueue = -1;
		
		for (int i = 0; i < job.getReduceTasks(); ++i) {
			bucketsQueued.add(new HashSet<Worker>());
			bucketsDone2.add(new HashSet<Integer>());
			workQueue.add(new Bucket(i));
		}
		
		this.allBucketsDone = false;
	}
	
	/**
	 * Tests whether all buckets are reduced.
	 * 
	 * @return True if all buckets are reduced and false if otherwise. 
	 */
	public boolean areAllBucketsDone()
	{
		return allBucketsDone;
	}
	
	/**
	 * Select next bucket to reduce. The bucket is selected by selecting a bucket
	 * from a work queue of non-reduced buckets. The BucketStore has an index pointing to the
	 * previously selected bucket that is updated when this call is made. The selected
	 * bucket will be set as queued for the worker in question.
	 * 
	 * @param toWhom The worker we are selecting a Bucket for.
	 * @return The next bucket to work on.
	 */
	public Bucket selectBucketToWorkOn(Worker toWhom)
	{
		previousIndexOfWorkQueue = (previousIndexOfWorkQueue + 1) % workQueue.size();

		Bucket work = workQueue.get(previousIndexOfWorkQueue);
		
		bucketsQueued.get(work.getId()).add(toWhom);
		
		return work;
	}

	/**
	 * Acknowledge that a worker has reduced a bucket. We save reducer IDs instead of reducer objects, as
	 * reduce task completition can not be undone. The acknowledged bucket is removed from the work queue
	 * and the queue index is updated as necessary. The method also updates the worker queue for the bucket
	 * in question (by removing this worker from the queue). 
	 * 
	 * @param w The worker who is acknowledging work
	 * @param b The bucket that is being acknowledged
	 */
	public void acknowledgeWork(Worker w, Bucket b)
	{
		
		bucketsDone2.get(b.getId()).add(w.hashCode());
		
		boolean test = true;
		for (Set<Integer> d : this.bucketsDone2) {
			if (d.isEmpty()) {
				test = false;
				break;
			}
		}
		if (test) {
			allBucketsDone = true;
		}
		
		bucketsQueued.get(b.getId()).remove(w);
		
		// Remove bucket b if it's in the work queue
		int i = workQueue.indexOf(b);
		if (i != -1) {
			workQueue.remove(i);
			// And fix work queue indexing if necessary
			if (i <= previousIndexOfWorkQueue) {
				previousIndexOfWorkQueue--;
			}
		}
			
			
	}
	
	/**
	 * Remove all data marked for the worker. Note that any data acknowledged by the
	 * worker has been saved to the FS, so there is no need to de-acknowledge them.
	 * 
	 * @param w The worker to be removed
	 */
	public void removeWorkerInformation(Worker w)
	{
		for (Set<Worker> workers : bucketsQueued) {
			workers.remove(w);
		}
	}

	/**
	 * Tells us whether a specific bucket has been reduced or not.
	 * 
	 * @param bucket The bucket in question.
	 * @return True if the bucket has been reduced, false if otherwise
	 */
	public boolean isBucketDone(Bucket bucket) 
	{
		Set<Integer> d = bucketsDone2.get(bucket.getId());
		boolean incomplete = d.isEmpty();
		return !incomplete;
	}
	
	/**
	 * Returns all workers for whom a bucket has been queued for.
	 * 
	 * @param bucket The bucket in question
	 * @return An unmodifiable set of workers for whom the bucket has been queued
	 */
	public Set<Worker> getAllQueuedWorkers(Bucket bucket)
	{
		return Collections.unmodifiableSet(bucketsQueued.get(bucket.getId()));
	}

	public Set<Integer> getAllDoneWorkers(Bucket bucket) {
		return Collections.unmodifiableSet(bucketsDone2.get(bucket.getId()));
	}

}
