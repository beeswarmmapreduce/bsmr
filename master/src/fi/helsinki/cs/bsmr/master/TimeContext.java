package fi.helsinki.cs.bsmr.master;

/**
 * The MIT License
 * 
 * Copyright (c) 2010   Department of Computer Science, University of Helsinki
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * Author Sampo Savolainen
 *
 */

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
