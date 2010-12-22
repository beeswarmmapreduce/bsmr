package fi.helsinki.cs.bsmr.fs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.util.ajax.JSON;

public class PartitionCombinatorServlet extends HttpServlet 
{
	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(PartitionCombinatorServlet.class.getCanonicalName());
	
	private String savePath;
	private Map<Integer, JobInfo> jobStore;
	
	private static final Charset charset = Charset.forName("UTF-8");
	
	@Override
	public void init(ServletConfig config) throws ServletException 
	{
		super.init(config);
		
		jobStore = new HashMap<Integer, JobInfo>();
		
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
		
		String []jobParts = null;
		if (path.length > 1) {
			jobParts = path[1].split("-");
		}
		
		if (path.length != 3 || jobParts.length != 2) {
			error("Specify path so that the url ends with /job-R/partition (both as integers)", req, resp);
			return;
		}
		
		int thisPartition = parseNumber(path[2],     "Partition", req, resp);
		int thisJob       = parseNumber(jobParts[0], "Job", req, resp);
		int thisR		  = parseNumber(jobParts[1], "Number of partitions", req, resp);
		
		if (thisPartition == -1 || thisJob == -1 || thisR == -1) {
			error("Non-integer or illegal values in the path", req, resp);
			return;
		}
		
		JobInfo job = null;
		
		synchronized (jobStore) {
			job = jobStore.get(thisJob);
			if (job == null) {
				job = new JobInfo(thisJob, thisR);
				jobStore.put(thisJob, job);
			}
			
		}
		
		if (job.isDone()) {
			logger.info("Someone tried to save a partition to a finished job.. ignoring");

			resp.setStatus(HttpURLConnection.HTTP_OK);
			PrintWriter w = resp.getWriter();
		
			w.println("OK (job already done)");
			w.flush();
			return;
		}
		
		job.savePartition(thisPartition, req, resp);
		
		
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
	
	private class JobInfo 
	{
		private int jobId;
		private int partitions;
		private int numKeys;
		private Set<Integer> savedPartitions;
		private boolean done;
		
		private String fileName;
		private Writer jobOutput;
		
		private JobInfo(int jobId, int partitions) throws IOException
		{
			this.numKeys = 0;
			this.done = false;
			this.jobId = jobId;
			this.partitions = partitions;
			this.savedPartitions = new HashSet<Integer>();
			
			this.fileName = savePath+"/"+this.jobId+".json"; 
			
			File f = new File(fileName);
			if (!f.createNewFile()) {
				logger.warning("Overwriting output file for job "+this.jobId);
			}
			
			this.jobOutput = new OutputStreamWriter(new FileOutputStream(f), charset);
			// Make sure there is enough room for any and all integer values 
			this.jobOutput.write("[             \n"); 
		}
		
		private void savePartition(int partition, HttpServletRequest req, HttpServletResponse resp) throws IOException 
		{
			if (partition < 0 || partition >= partitions) {
				error("Partition "+partition+" is out of bounds! (this job has "+partitions+" partitions)", req, resp);
				return;
			}
			
			synchronized (savedPartitions) {
				
				if (savedPartitions.contains(partition)) {

					resp.setStatus(HttpURLConnection.HTTP_OK);
					PrintWriter w = resp.getWriter();
				
					w.println("OK (partition was already saved)");
					w.flush();
					return;
				}
				
				try {
					storeKeys(req.getInputStream());
				} catch(IOException ie) {
					logger.log(Level.SEVERE, "Could not store keys for partition "+partition+" (job "+jobId+")", ie);
					error(ie.getMessage(), req,resp);
					return;
				}
				
				savedPartitions.add(partition);
				
				if (savedPartitions.size() >= partitions) {
					jobOutput.write("]\n");
					jobOutput.flush();
					jobOutput.close();
					
					// Write the number of keys
					RandomAccessFile f = new RandomAccessFile(fileName, "rwd"); // make sure all updates are immediate
					f.seek(2);
					f.write(new Integer(numKeys).toString().getBytes(charset));
					f.close();
					
					done = true;
				}
				
				resp.setStatus(HttpURLConnection.HTTP_OK);
				PrintWriter w = resp.getWriter();
			
				w.println("OK");
				w.flush();
			}
		}
		
		private boolean isDone()
		{
			return done;
		}
		
		private void storeKeys(InputStream is) throws IOException
		{			
			try {
				InputStreamReader isr = new InputStreamReader(is, charset);
				Map<Object, Object> keys = (Map<Object, Object>)JSON.parse(isr);
			
				int i = 0;
				Map<Object, Object> tmp = new HashMap<Object, Object>();
				for (Object key : keys.keySet()) {
					i++;
					tmp.clear();
					tmp.put(key, keys.get(key));
					
					jobOutput.write(",");
					jobOutput.write(JSON.toString(tmp));
					jobOutput.write("\n");
				}
				
				
				logger.fine("Read "+i+" keys from the client");
				numKeys += i;
			
			} finally {
				if (is != null) { is.close(); }
			}
		}
	}
}
