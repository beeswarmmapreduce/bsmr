package fi.helsinki.cs.bsmr.fs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.util.zip.GZIPInputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class SplitServlet extends HttpServlet 
{
	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException 
	{
		String []path = req.getPathInfo().split("/");
		
		if (path.length != 3) {
			error("Specify path so that the url ends with /[job]/[split] (both as integers)", req, resp);
			return;
		}
		
		String jobAsString   = path[1];
		String splitAsString = path[2];
		int thisJob;
		int thisSplit;
		
		try {
			thisSplit = Integer.parseInt(splitAsString);
		} catch(NullPointerException npe) {
			error("Specify path so that the url ends with /[job]/[split] (both as integers)", req, resp);
			return;
		} catch(NumberFormatException nfe) {
			error("Split ("+splitAsString+") is not na number: "+nfe, req, resp);
			return;
		}
		
		try {
			thisJob = Integer.parseInt(jobAsString);
		} catch(NullPointerException npe) {
			error("Specify path so that the url ends with /[job]/[split] (both as integers)", req, resp);
			return;
		} catch(NumberFormatException nfe) {
			error("Job ("+jobAsString+") is not a number: "+nfe, req, resp);
			return;
		}
		
		retrieveSplit(thisJob, thisSplit, req, resp);
	}
	
	private void retrieveSplit(int job, int split, HttpServletRequest req, HttpServletResponse resp) 
		throws IOException
	{
		// NOTE: the job parameter is not used
		InputStream is = this.getClass().getClassLoader().getResourceAsStream("data.txt.gz");
		if (is == null) {
			error("could not open data.txt.gz", req, resp);
			return;
		}
		
		BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(is))); 
		br.skip(split * 32*1024);
		
		String s = br.readLine();
		
		if (s == null) {
			error("not enough data in input file", req, resp);
			return;
		}
		
		resp.setContentType("text/plain");
		PrintWriter writer = resp.getWriter();
		
		int n = 0;
		
		while (n < 32*1024 && s != null) {
			writer.println(s);
			n+= s.length();
			s = br.readLine();
		}
		
		writer.close();
	}

	private void error(String msg, HttpServletRequest req, HttpServletResponse resp) throws IOException
	{
		resp.setStatus(HttpURLConnection.HTTP_INTERNAL_ERROR);
		resp.setContentType("text/plain");
		PrintWriter pw = resp.getWriter();
		pw.println("ERROR: "+msg);
		pw.close();
	}
}
