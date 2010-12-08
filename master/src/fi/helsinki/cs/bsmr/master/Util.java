package fi.helsinki.cs.bsmr.master;

import java.util.logging.Logger;

/**
 * Miscellaneous utility functions. 
 * 
 * @author stsavola
 *
 */
public class Util
{
	/**
	 * Retrieves the Java Logger object. This convenience method is only to allow for 
	 * centralized code-level control of the properties in loggers.  
	 * 
	 * @param clazz The class for which we want a logger for.
	 * @return The logger object for
	 */
	public static Logger getLoggerForClass(Class<?> clazz)
	{
		return Logger.getLogger(clazz.getCanonicalName());
	}
	
	/**
	 * Turn an object stored in a JSON data structure into an int. This is done
	 * by identifying the object as either a Long, Integer or a String.
	 * 
	 * @param o The object to be parsed as an integer
	 * @return The object o as an integer
	 * @throws NumberFormatException If the object is null or not an instance of 
	 *                               any of the supported classes.
	 */
	public static int getIntFromJSONObject(Object o) throws NumberFormatException
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
