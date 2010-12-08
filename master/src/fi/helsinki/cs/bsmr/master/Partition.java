package fi.helsinki.cs.bsmr.master;

import java.io.Serializable;

/**
 * A partition for reducing. 
 * 
 * @author stsavola
 *
 */
public class Partition implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	private int id;
	
	public Partition(int id)
	{
		this.id = id;
	}
	
	public int getId()
	{
		return id;
	}

	public String toString()
	{
		return "PartId: "+id;
	}
	
	@Override
	public int hashCode() {
		return Partition.class.hashCode() ^ id;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (! (obj instanceof Partition)) return false;
		Partition b = (Partition)obj;
		
		return b.id == id;
	}
}
