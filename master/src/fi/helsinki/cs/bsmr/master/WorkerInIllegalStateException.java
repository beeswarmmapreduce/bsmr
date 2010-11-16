package fi.helsinki.cs.bsmr.master;

public class WorkerInIllegalStateException extends Exception 
{
	private static final long serialVersionUID = 1L;

	public WorkerInIllegalStateException(String msg)
	{
		super(msg);
	}
}
