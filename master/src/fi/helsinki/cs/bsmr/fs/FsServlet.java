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
import java.io.RandomAccessFile;
import java.io.FileInputStream;

import java.io.IOException;
import java.util.logging.Logger;


import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


/**
 * This servlet implements a simple filesystem with read, write and ls operations as a servlet.
 * Read and write operations additionally support addressing a range of bytes in the file.
 * The servlet is, at least initially, non-RESTful for the ease of implementation and testing 
 *
 * Get / post Parameters
 *
 * operation = read | write
 * filename = [string]
 * begin = [integer]		//inclusive begin offset  
 * length = [integer]			//number of bytes to read or write 
 *  
 *  Example URL: http://localhost:8080/fs/filesystem?operation=read&filename=alphabet.txt&begin=2&length=3
 *  
 * @author pzsavola
 * @see FsServlet
 */
public class FsServlet extends HttpServlet
{
private static final long serialVersionUID = 1L;

private static Logger logger = Logger.getLogger(FsServlet.class.getCanonicalName());
	


String workingDir = null; 

@Override
public void init(ServletConfig config) throws ServletException 
	{
	super.init(config);
		
	String loadPath = config.getInitParameter("loadPath");
	
	loadPath = Util.fixPathSeparators(loadPath); 
	
	
	if (loadPath != null && loadPath.charAt(loadPath.length()-1) != File.separatorChar) 
		{
		loadPath = loadPath + File.separator;
		}
		
	
	
	logger.info("loadPath: '"+loadPath+"'");
		
	if (loadPath != null) 
		{
		File loadHandle = new File(loadPath);
		this.workingDir = loadPath;
		
		if (loadHandle.exists() && !loadHandle.isDirectory()) 
			{
			logger.severe("loadPath points to a non-directory! disabling loading");
			this.workingDir = null;
			return;
			}
			
		if (!loadHandle.exists()) 
			{
			logger.severe("loadPath does not exist! disabling loading");
			this.workingDir = null;
			return;
			}
		}
		

	}

private void handleRead(String filename, HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
	{
	RandomAccessFile input = null;
	ServletOutputStream output = null;
	int beginOffset = 0; 
	int length = -1;
	
	try	{
		beginOffset = Integer.parseInt(req.getParameter("begin"));
		}
	catch (Exception e)
		{}
	
	try	{
		length = Integer.parseInt(req.getParameter("length"));
		}
	catch (Exception e)
		{}
	
	try	{
		
		input = new RandomAccessFile(filename, "r"); 
		output = resp.getOutputStream();
	
		if (beginOffset>0)
			{
			input.seek(beginOffset);
			}
		
		
		byte[] buffer = new byte[4096];
		int bytesRead = 0;
		int totalRead = 0;
		int totalWritten = 0;
		
		while ((bytesRead = input.read(buffer)) != -1)
			{
			totalRead += bytesRead;
			if (length != -1 && totalRead > length) 
				{
				output.write(buffer, 0, length-totalWritten);  // write
				break;
				}
			output.write(buffer, 0, bytesRead); // write
			totalWritten += bytesRead;
			}
		}

	catch (IOException e2)
		{
		logger.severe(e2.toString());
		throw e2;
		}

	finally
		{
		if (input!=null)
			input.close();
		
		if (output!=null)
			{	
			output.flush();
			output.close();
			}
		}
	}

private void handleWrite(String filename, HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
	{

	}

private void handleLs(String filename, HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
	{

	}

@Override
protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException 
	{
	String operation = req.getParameter("operation");
	String filename = req.getParameter("filename");
	
	filename = Util.fixPathSeparators(filename);
	
	filename = this.workingDir+File.separator+filename;
	
	//req.getParameter("beginIndex");
	//req.getParameter("endIndex");		//exclusive
	
	
	if ("read".equalsIgnoreCase(operation))
		this.handleRead(filename, req, resp);
	
	if ("write".equalsIgnoreCase(operation))
		this.handleWrite(filename, req, resp);
	
	if ("write".equalsIgnoreCase(operation))
		this.handleLs(filename, req, resp);
	
	}


@Override
protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException 
	{
	this.doGet(req, resp);
	}

}
