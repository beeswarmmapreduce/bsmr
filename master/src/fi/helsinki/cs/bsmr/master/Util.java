package fi.helsinki.cs.bsmr.master;

import java.util.logging.Logger;

public class Util
{
	public static Logger getLoggerForClass(Class<?> clazz)
	{
		return Logger.getLogger(clazz.getCanonicalName());
	}
}
