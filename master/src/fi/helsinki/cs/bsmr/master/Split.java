package fi.helsinki.cs.bsmr.master;

import java.io.Serializable;

public class Split implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	private int id;
	
	public Split(int id)
	{
		this.id = id;
	}
	
	public int getId()
	{
		return id;
	}


	@Override
	public int hashCode() {
		return Split.class.hashCode() & id;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (! (obj instanceof Split)) return false;
		Split b = (Split)obj;
		
		return b.id == id;
	}
}
