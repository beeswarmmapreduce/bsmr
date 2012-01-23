package fi.helsinki.cs.bsmr.fs;

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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
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

import fi.helsinki.cs.bsmr.master.JSON;

/**
 * A servlet to save BSMR partitions from workers. The workers POST JSON dicts and the servlet appends
 * all keys into a special JSON file on the disk. Workers should address the servlet 
 * /[jobId]-[R]/[partition]. First part contains the job id of the partition
 * 
 * The file will be a list where the first entry is the
 * number of keys there is in the list. All following entries are single-key dicts.
 * 
 * <pre>
 * [ 4           
 * ,{"scrooge":"1"}
 * ,{"mickey":"4321"}
 * ,{"pluto":"isadog"}
 * ,{"donald":"12345"}
 * ]
 * </pre>
 * 
 * The file must be in this format so the SplitServlet can serve it efficiently.
 * 
 * @author stsavola
 * @see SplitServlet
 *
 */
public class PartitionServlet extends HttpServlet 
{
	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(PartitionServlet.class.getCanonicalName());
	
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
			Util.error("Saving disabled!",req,resp);
			return;
		}
		
		String []path = req.getPathInfo().split("/");
		
		String []jobParts = null;
		if (path.length > 1) {
			jobParts = path[1].split("-");
		}
		
		if (path.length != 3 || jobParts.length != 2) {
			Util.error("Specify path so that the url ends with /[jobId]-[R]/[partition]", req, resp);
			return;
		}
		
		int thisPartition = Util.parseNumber(path[2],     "/[jobId]-[R]/[partition]", "Partition", req, resp);
		int thisJob       = Util.parseNumber(jobParts[0], "/[jobId]-[R]/[partition]", "Job", req, resp);
		int thisR		  = Util.parseNumber(jobParts[1], "/[jobId]-[R]/[partition]", "Number of partitions", req, resp);
		
		if (thisPartition == -1 || thisJob == -1 || thisR == -1) {
			Util.error("Non-integer or illegal values in the path", req, resp);
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
			
			logger.fine("Creating result file "+fileName);
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
				Util.error("Partition "+partition+" is out of bounds! (this job has "+partitions+" partitions)", req, resp);
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
					Util.error(ie.getMessage(), req,resp);
					return;
				}
				
				savedPartitions.add(partition);
				
				if (savedPartitions.size() >= partitions) {
					logger.fine("Finalizing output file "+fileName);
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
		
		@SuppressWarnings("unchecked")
		private void storeKeys(InputStream is) throws IOException
		{		
			StringBuffer bufferedMessage = new StringBuffer();
			int i;
			
			try {
				
				
				InputStreamReader isr = new InputStreamReader(is, charset);
				char [] buf = new char[256];
				
				while ( (i = isr.read(buf)) > 0) {
					bufferedMessage.append(buf, 0, i);
				}
				
				Object jsonObj = JSON.parse(bufferedMessage.toString());
				
				
				Map<Object, Object> keys;
				if (jsonObj instanceof Map<?, ?>) {
					keys = (Map<Object, Object>)jsonObj;
				} else {
					if (jsonObj == null) {
						throw new IOException("JSON object is null?!?!");
					} else {
						throw new IOException("JSON Object not a map! "+jsonObj.getClass().getName()+": "+jsonObj);
					}
				}
				
			
				i = 0;
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
			
			} catch(RuntimeException re) {
				logger.log(Level.SEVERE, "Throwable when parsing partition. Partition message: '"+bufferedMessage.toString()+"'", re);
				throw re;
			} finally {
				if (is != null) { is.close(); }
			}
		}
	}
}
