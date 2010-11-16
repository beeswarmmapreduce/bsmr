package fi.helsinki.cs.bsmr.master;

public class JobAlreadyRunningException extends Exception 
{
	private static final long serialVersionUID = 1L;
	
	public JobAlreadyRunningException(String msg)
	{
		super(msg);
	}

}
