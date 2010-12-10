package fi.helsinki.cs.bsmr.fs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * A naive servlet which saves partitions to the local filesystem.
 * 
 * @author stsavola
 *
 */
public class PartitionServlet extends HttpServlet 
{
	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(PartitionServlet.class.getCanonicalName());
	
	private String savePath;
	
	@Override
	public void init(ServletConfig config) throws ServletException 
	{
		super.init(config);
		
		savePath = config.getInitParameter("savePath");
		
		if (savePath != null && savePath.charAt(savePath.length()-1) != '/') {
			savePath = savePath + '/';
		}
		
		logger.info("savePath: '"+savePath+"'");
		
		if (savePath != null) {
			File saveDir = new File(savePath);
			
			if (saveDir.exists() && !saveDir.isDirectory()) {
				logger.severe("savePath points to a non-directory! disabling saving");
				savePath = null;
				return;
			}
			
			if (!saveDir.exists()) {
				if (saveDir.mkdir()) {
					logger.info("savePath created");
				} else {
					logger.severe("Could not create savePath! disabling saving");
					savePath = null;
					return;
				}
			}

			
			File testFile = new File(savePath+"thisisjustatest");
			try {
				
				if (testFile.createNewFile()) {
					logger.info("Test file created OK");
					testFile.delete(); // remove the test file
				} else {
					logger.info("Could not create test file, disabling saving!");
				}
				
			} catch(IOException ie) {
				logger.log(Level.SEVERE, "Problem with savePath. Disabling saving for now!!", ie);
				savePath = null;
			}
		}
		
		
	}
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException
	{
		String []path = req.getPathInfo().split("/");
		
		if (path.length != 3) {
			error("Specify a path so that the url ends with /job/partition (both as integers)", req, resp);
			return;
		}
		
		// Do String -> integer conversion so that we don't allow nasty paths containing for example ..'s 
		int thisPartition = parseNumber(path[2], "Partition", req, resp);
		int thisJob       = parseNumber(path[1], "Job", req, resp);
		
		FileInputStream fis = new FileInputStream(savePath+"/"+thisJob+"/"+thisPartition+".data");
		resp.setContentType("text/plain");
		resp.addHeader("Content-Disposition", "inline;filename=bsmr_output_j"+thisJob+"_p"+thisPartition+".txt");
		OutputStream os = resp.getOutputStream();
		byte [] buf = new byte[32*1024];
		
		int i;
		i = fis.read(buf);
		
		while(i > 0) {
			os.write(buf, 0, i);
			i = fis.read(buf);
		}
		os.flush();
		os.close();
	}
	
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException 
	{
		if (savePath == null) {
			error("Saving disabled!",req,resp);
			return;
		}
		
		String []path = req.getPathInfo().split("/");
		
		if (path.length != 3) {
			error("Specify path so that the url ends with /job/partition (both as integers)", req, resp);
			return;
		}
		
		
		int thisPartition = parseNumber(path[2], "Partition", req, resp);
		int thisJob       = parseNumber(path[1], "Job", req, resp);
		
		
		savePartition(thisPartition, thisJob, req, resp);
	}
	
	private void savePartition(int partition, int job, HttpServletRequest req, HttpServletResponse resp) throws IOException 
	{
		
		InputStream is = req.getInputStream();
		FileOutputStream fos = null;
		
		try {
			File dir = new File(savePath+"/"+job);
			if (!dir.isDirectory()) {
				if (!dir.mkdir()) {
					logger.severe("Could not create directory for job "+job);
					error("Could not save!", req, resp);
					return;
				}
			}
			
			File f = new File(savePath+"/"+job+"/"+partition+".data");
			if (!f.createNewFile()) {
				logger.warning("Overwriting J="+job+", P="+partition);
			}
			fos = new FileOutputStream(f);
			
			
			byte []buf = new byte[32*1024];
			int n = 0;
			int i;
			
			do {
				i = is.read(buf);
				if (i < 1) break;
				
				fos.write(buf, 0, i);
				n += i;
			} while (true);
			
			logger.info("Read "+n+" bytes from the client");
			resp.setStatus(HttpURLConnection.HTTP_OK);
			PrintWriter w = resp.getWriter();
		
			w.println("OK");
			w.flush();
		} catch(IOException ie) {
			error("Could not save! "+ie, req, resp);
			throw ie;
		} finally {
			if (fos != null) { fos.close(); }
			if (is != null) { is.close(); }
		}
	}

	private static void error(String msg, HttpServletRequest req, HttpServletResponse resp) throws IOException
	{
		resp.setStatus(HttpURLConnection.HTTP_INTERNAL_ERROR);
		resp.setContentType("text/plain");
		PrintWriter pw = resp.getWriter();
		pw.println("ERROR: "+msg);
		pw.close();
	}
	
	private static int parseNumber(String s, String nameOfNumber, HttpServletRequest req, HttpServletResponse resp) throws IOException
	{
		int ret;
		try {
			ret = Integer.parseInt(s);
		} catch(NullPointerException npe) {
			error("Specify path so that the url ends with /job/partition (both as integers)", req, resp);
			return -1;
		} catch(NumberFormatException nfe) {
			error(nameOfNumber+" ("+s+") is not a number: "+nfe, req, resp);
			return -1;
		}
		return ret;
	}
}
