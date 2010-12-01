package fi.helsinki.cs.bsmr.master;

import java.util.logging.Logger;

public class Util
{
	public static Logger getLoggerForClass(Class<?> clazz)
	{
		return Logger.getLogger(clazz.getCanonicalName());
	}
	
	public static int getIntFromJSONObject(Object o)
	{
		if (o instanceof Long) {
			return ((Long)o).intValue();
		}
		
		if (o instanceof Integer) {
			return ((Integer)o).intValue();
		}
		
		if (o instanceof String) {
			return Integer.parseInt((String)o); 
		}
		
		if (o == null) {
			throw new NumberFormatException("null is not a number!");
		}
			
		
		throw new NumberFormatException("Object "+o+" cannot be parsed into an int!");
	}
}
