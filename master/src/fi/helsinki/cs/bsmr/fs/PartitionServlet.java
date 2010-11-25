package fi.helsinki.cs.bsmr.fs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
		
		String jobAsString       = path[1];
		String partitionAsString = path[2];
		
		int thisPartition;
		int thisJob;
		
		try {
			thisPartition = Integer.parseInt(partitionAsString);
		} catch(NullPointerException npe) {
			error("Specify path so that the url ends with /job/partition (both as integers)", req, resp);
			return;
		} catch(NumberFormatException nfe) {
			error("Partition ("+partitionAsString+") is not a number: "+nfe, req, resp);
			return;
		}
		
		try {
			thisJob = Integer.parseInt(jobAsString);
		} catch(NullPointerException npe) {
			error("Specify path so that the url ends with /job/partition (both as integers)", req, resp);
			return;
		} catch(NumberFormatException nfe) {
			error("Job ("+jobAsString+") is not a number: "+nfe, req, resp);
			return;
		}
		
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

	private void error(String msg, HttpServletRequest req, HttpServletResponse resp) throws IOException
	{
		resp.setStatus(HttpURLConnection.HTTP_INTERNAL_ERROR);
		resp.setContentType("text/plain");
		PrintWriter pw = resp.getWriter();
		pw.println("ERROR: "+msg);
		pw.close();
	}
}
