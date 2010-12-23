package fi.helsinki.cs.bsmr.fs;


import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface SplitSource
{
	
	public void retrieveWikiSplit(Object inputRef, int splitCount, int split, HttpServletRequest req, HttpServletResponse resp) 
		throws IOException;

}
