package fi.helsinki.cs.bsmr.master;

public class IllegalMessageException extends Exception 
{
	private static final long serialVersionUID = 1L;
	
	private String problematicMessage;
	

	public IllegalMessageException(String error)
	{
		super(error);
	}
	
	
	public IllegalMessageException(String error, String problematicMessage)
	{
		super(error);
		this.problematicMessage = problematicMessage;
	}
	
	public String getProblematicMessage()
	{
		return problematicMessage;
	}
	

	public void setProblematicMessage(String problematicMessage)
	{
		this.problematicMessage = problematicMessage;
	}
}
