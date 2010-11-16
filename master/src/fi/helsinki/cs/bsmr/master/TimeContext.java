package fi.helsinki.cs.bsmr.master;

/**
 * Stores a single time value for the duration of one call so that timeout resolving is coherent.
 *  
 * @author stsavola
 *
 */
public class TimeContext 
{
	private static ThreadLocal<TimeContext> threadLocalTimeContext = new ThreadLocal<TimeContext>() {
		protected TimeContext initialValue() {
			return new TimeContext();
		}
	};

	private static TimeContext getInstance()
	{
		return threadLocalTimeContext.get();
	}
	
	
	private long now;
	
	private TimeContext()
	{
		markTime();
	}
	
	public static void markTime()
	{
		getInstance().now = System.currentTimeMillis();
	}
	
	public static long now()
	{
		return getInstance().now;
	}

	

}
