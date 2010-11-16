package fi.helsinki.cs.bsmr.master;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public class AvailableWorkerSet implements Set<Worker> 
{
	private Set<Worker> superSet;
	private Job job;
	
	public AvailableWorkerSet(Set<Worker> set, Job job) 
	{
		this.superSet = set;
		this.job      = job;
	}


	@Override
	public boolean contains(Object arg0) {
		Worker w = (Worker)arg0;
		return (superSet.contains(w) && w.isAvailable(job));
	}

	@Override
	public boolean containsAll(Collection<?> arg0)
	{
		for (Object o : arg0) {
			if (! (o instanceof Worker)) { return false; }
			Worker w = (Worker)o;
			
			if (!superSet.contains(w) || !w.isAvailable(job)) {
				return false;
			}
		
		}
		return true;
	}

	@Override
	public boolean isEmpty()
	{
		for (Worker w : superSet) {
			if (w.isAvailable(job)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public Iterator<Worker> iterator()
	{
		return new AvailableWorkerIterator();
	}
	
	private class AvailableWorkerIterator implements Iterator<Worker>
	{
		private Worker next;
		private Iterator<Worker> iter;
		
		private AvailableWorkerIterator() 
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
			} while (!next.isAvailable(job));
		}
		
		
	}

	/** Unimplemented Set<T> methods **/

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
