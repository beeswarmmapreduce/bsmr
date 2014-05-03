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

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * This is a Set wrapper which disregards workers who are known to be unreachable.
 * The reachability status reported by workers does not affect this. Using this
 * structure there is no need to copy the full set of workers and then remove unavailable
 * workers. Note that only a subset of Set functionality is supported. Unsupported
 * functions will throw a RuntimeException
 * 
 * @author stsavola
 *
 */
public class ReachableWorkerSet implements Set<Worker> 
{
	private Set<Worker> superSet;
	private Job job;
	
	/**
	 * Create a set of workers which filters out the workers who are unavailable.
	 * 
	 * @param set The full set
	 * @param job The job whose heart beat and acknowledgment timeout parameters are used 
	 *            to classify worker state.
	 */
	public ReachableWorkerSet(Set<Worker> set, Job job) 
	{
		this.superSet = set;
		this.job      = job;
	}


	@Override
	public boolean contains(Object o) {
		Worker w = (Worker)o;
		return (superSet.contains(w) && w.isReachable(job));
	}

	@Override
	public boolean containsAll(Collection<?> arg0)
	{
		for (Object o : arg0) {
			if (! (o instanceof Worker)) { return false; }
			Worker w = (Worker)o;
			
			if (!superSet.contains(w) || !w.isReachable(job)) {
				return false;
			}
		
		}
		return true;
	}

	@Override
	public boolean isEmpty()
	{
		for (Worker w : superSet) {
			if (w.isReachable(job)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public Iterator<Worker> iterator()
	{
		return new ReachableWorkerIterator();
	}
	
	private class ReachableWorkerIterator implements Iterator<Worker>
	{
		private Worker next;
		private Iterator<Worker> iter;
		
		private ReachableWorkerIterator() 
		{
			iter = superSet.iterator();
			windUp();
		}
		
		@Override
		public boolean hasNext() 
		{
			return next != null;
		}

		@Override
		public Worker next() {
			Worker ret = next;
			windUp();
			return ret;
		}

		@Override
		public void remove()
		{
			throw new RuntimeException("Not implemented!");
		}
		
		private void windUp()
		{
			do {
				if (!iter.hasNext()) {
					next = null;
					return;
				}
				next = iter.next();
			} while (!next.isReachable(job));
		}
		
		
	}

	// Unimplemented Set methods

	@Override
	public boolean add(Worker arg0) 
	{
		throw new RuntimeException("Not implemented!");
	}

	@Override
	public boolean addAll(Collection<? extends Worker> arg0) 
	{
		throw new RuntimeException("Not implemented!");
	}

	@Override
	public void clear()
	{
		throw new RuntimeException("Not implemented!");
	}
	
	@Override
	public boolean remove(Object arg0) 
	{
		throw new RuntimeException("Not implemented!");
	}

	@Override
	public boolean removeAll(Collection<?> arg0) {
		throw new RuntimeException("Not implemented!");
	}

	@Override
	public boolean retainAll(Collection<?> arg0) {
		throw new RuntimeException("Not implemented!");
	}

	@Override
	public int size() {
		throw new RuntimeException("Not implemented!");
	}

	@Override
	public Object[] toArray() {
		throw new RuntimeException("Not implemented!");
	}

	@Override
	public <T> T[] toArray(T[] arg0) {
		throw new RuntimeException("Not implemented!");
	}

}
