package fi.helsinki.cs.bsmr.fs;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class Util 
{
	public static int parseNumber(String s, String format, String nameOfNumber, HttpServletRequest req, HttpServletResponse resp) throws IOException
	{
		int ret;
		try {
			ret = Integer.parseInt(s);
		} catch(NullPointerException npe) {
			error("Specify path so that the url ends with "+format+" (as integers)", req, resp);
			return -1;
		} catch(NumberFormatException nfe) {
			error(nameOfNumber+" ("+s+") is not a number: "+nfe, req, resp);
			return -1;
		}
		return ret;
	}
	
	public static void error(String msg, HttpServletRequest req, HttpServletResponse resp) throws IOException
	{
		resp.setStatus(HttpURLConnection.HTTP_INTERNAL_ERROR);
		resp.setContentType("text/plain");
		PrintWriter pw = resp.getWriter();
		pw.println("ERROR: "+msg);
		pw.close();
	}
	
}
