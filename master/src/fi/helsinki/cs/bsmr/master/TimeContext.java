package fi.helsinki.cs.bsmr.master;

/**
 * Stores a single time value for the duration of one call so that Worker availability resolving is coherent.
 *  
 * @author stsavola
 */
public class TimeContext 
{
	/**
	 * Store a single TimeContext object per thread
	 */
	private static ThreadLocal<TimeContext> threadLocalTimeContext = new ThreadLocal<TimeContext>() {
		protected TimeContext initialValue() {
			return new TimeContext();
		}
	};

	/**
	 * Get the TimeContext object for the caller thread.
	 * 
	 * @return The TimeContext for this thread.
	 */
	private static TimeContext getInstance()
	{
		return threadLocalTimeContext.get();
	}
	
	
	private long now;
	
	private TimeContext()
	{
	}
	
	/**
	 * Mark "now" as the current time for this thread. All subsequent calls to now() will return
	 * the time marked by this call.
	 * 
	 * @see TimeContext#now
	 */
	public static void markTime()
	{
		getInstance().now = System.currentTimeMillis();
	}
	
	/**
	 * Returns the current time as set by the last call to markTime() for this thread.
	 * 
	 * @return Current time in milliseconds
	 * @see TimeContext#markTime
	 */
	public static long now()
	{
		return getInstance().now;
	}

	

}
