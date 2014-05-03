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
