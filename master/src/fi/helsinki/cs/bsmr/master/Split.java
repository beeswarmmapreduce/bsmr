package fi.helsinki.cs.bsmr.master;

public class Split 
{
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
