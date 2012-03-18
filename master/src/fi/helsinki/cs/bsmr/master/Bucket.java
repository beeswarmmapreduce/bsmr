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

import java.io.Serializable;

/**
 * A bucket for reducing. 
 * 
 * @author stsavola
 *
 */
public class Bucket implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	private int id;
	
	public Bucket(int id)
	{
		this.id = id;
	}
	
	public int getId()
	{
		return id;
	}

	public String toString()
	{
		return "BuckId: "+id;
	}
	
	@Override
	public int hashCode() {
		return Bucket.class.hashCode() ^ id;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (! (obj instanceof Bucket)) return false;
		Bucket b = (Bucket)obj;
		
		return b.id == id;
	}
}
