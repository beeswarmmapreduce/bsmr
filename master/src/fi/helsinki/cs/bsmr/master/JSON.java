package fi.helsinki.cs.bsmr.master;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.codehaus.jackson.map.ObjectMapper;


public class JSON {
	private static Logger logger = Logger.getLogger(JSON.class.getCanonicalName());
	
	public static Map<?,?> getJSONMapForJob(String s)
	{
		ObjectMapper mapper = new ObjectMapper();
		
		Map<?, ?> ret;
		try {
			ret =  mapper.readValue(s, Map.class);
		} catch(Exception e) {
			logger.log(Level.SEVERE, "Could not parse JSON string '"+s+"'", e);
			return null; 
		}
		
		//System.out.println("got: "+ret);
		
		return ret;
	}
	
	public static Object parse(String s)
	{
		return getJSONMapForJob(s);
	}

	
	public static String toString(Object o)
	{
		
		ObjectMapper mapper = new ObjectMapper();
		
		String ret;
		try {
			ret = mapper.writeValueAsString(o);
		} catch(Exception e) {
			logger.log(Level.SEVERE, "Could not parse object to JSON '"+o+"'", e);
			return null; 
		}
		//System.out.println("sending: "+ret);
		return ret;
	}
	
	
}
