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
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException 
	{
		int thisSplit = Integer.parseInt(req.getParameter("S"));
		
		InputStream is = this.getClass().getClassLoader().getResourceAsStream("data.txt.gz");
		if (is == null) {
			error("could not open data.txt.gz", req, resp);
			return;
		}
		
		BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(is))); 
		br.skip(thisSplit * 32*1024);
		
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
