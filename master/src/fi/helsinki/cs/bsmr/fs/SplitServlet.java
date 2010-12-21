package fi.helsinki.cs.bsmr.fs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * A naive servlet which servers parts of a static file on the local filesystem.
 * 
 * @author stsavola
 *
 */
public class SplitServlet extends HttpServlet 
{
	private static final long serialVersionUID = 1L;

	public static final int SPLIT_SIZE = 32*1024;

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
		long size = SPLIT_SIZE; // TODO: this should be calculated based on file size and number of splits
		long skip = split * size;
		
		InputStream is = this.getClass().getClassLoader().getResourceAsStream("data.txt.gz");
		if (is == null) {
			error("could not open data.txt.gz", req, resp);
			return;
		}
		
		is = new GZIPInputStream(is);
		
		Charset utf8 = Charset.forName("UTF-8");
		ByteSeekableCharacterInputStream bscis = new ByteSeekableCharacterInputStream(is);
		BufferedReader br = new BufferedReader(new InputStreamReader(bscis, utf8));
		
		String s;
		int i = 0; // How many BYTES have been read
		
		// Only skip if necessary
		if (skip > 0) {
			// Skip in the BYTE stream, not character stream
			bscis.skip(skip);
			
			// FFWD any BYTES from a previous UTF-8 character (if skip offset was mid-character)
			i += bscis.skipPartialUTF8Character();
			
			// Read the remaining part of the line our offset dropped us on
			s = br.readLine(); // This line we skip, as it is part of the previous split
			
			if (s == null) {
				// We've read past the end of file, or at least the previous split ended at EOF
				error("not enough data in input file for split "+split, req, resp);
				return;
			}
			
			// That line is ignored
			
			// Except for the amount of bytes it contained
			i += s.getBytes(utf8).length;
			i ++; // newline!
		}
		
		// Note! If the previously read last part of the line is longer than 'size', this
		// split will be empty. This is the correct behavior! Otherwise there would be overlaps
		// between skips
		
		resp.setContentType("text/plain");
		resp.setCharacterEncoding("UTF-8");
		PrintWriter out = resp.getWriter();
		
		while (i< size) {
			s = br.readLine();
			
			if (s == null) break; // EOF
						
			out.write(s);
			out.write('\n');
			
			i += s.getBytes(utf8).length;
			i ++; // newline!
		}
		out.flush();
		out.close();
		
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
