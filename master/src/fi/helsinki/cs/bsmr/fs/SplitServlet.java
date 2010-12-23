package fi.helsinki.cs.bsmr.fs;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;


import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


/**
 * A servlet which serves two different styles of input files to BSMR workers. Sources can be
 * either raw UTF8 files or JSON dicts saved in the format used by PartitionServlet. Workers need
 * to address the splits /[inputRef]-[M]/[split] where inputRef is either an integer for the JSON
 * dicts from previous runs or a non-integer for UTF-8 files. M is the number of splits in this job
 * and split is the number of split the worker wants (0..M-1).
 * 
 * If the inputRef is an integer, replies will be JSON dicts otherwise the output is RAW UTF-8 data.
 * For efficiency reasons, the servlet does not parse the JSON dicts on disk as JSON. Instead if relies
 * on the file containing the number of keys on the first line. Remaining lines should only contain one
 * key in its' own dict. For example:
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
 * The values for keys can be of any legal JSON format. The only constraint is that the complete dict 
 * and the surrounding {} brackets have to fit into one line. 
 * 
 * Note that due to the size of the enwiki.xml.gz, the size of that file is hard-coded.
 * 
 * @author stsavola
 * @see PartitionServlet
 */
public class SplitServlet extends HttpServlet
{
	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(SplitServlet.class.getCanonicalName());
	
	private SplitSource chainFSSource;
	private SplitSource hugeFileFSSource;

	@Override
	public void init(ServletConfig config) throws ServletException 
	{
		super.init(config);
		
		String loadPath = config.getInitParameter("loadPath");
		
		if (loadPath != null && loadPath.charAt(loadPath.length()-1) != '/') {
			loadPath = loadPath + '/';
		}
		
		logger.info("loadPath: '"+loadPath+"'");
		
		if (loadPath != null) {
			File saveDir = new File(loadPath);
			
			if (saveDir.exists() && !saveDir.isDirectory()) {
				logger.severe("loadPath points to a non-directory! disabling loading");
				loadPath = null;
				return;
			}
			
			if (!saveDir.exists()) {
				logger.severe("loadPath does not exist! disabling loading");
				loadPath = null;
				return;
			}
		}
		
		chainFSSource = new ChainSplitSourceImpl(loadPath);
		hugeFileFSSource = new HugeUTF8SplitSourceImpl(loadPath);
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException 
	{
		String []path = req.getPathInfo().split("/");
		String []idAndM = null;
		
		if (path.length > 1) {
			idAndM = path[1].split("-");
		}
		
		if (path.length != 3 || idAndM.length != 2) {
			Util.error("Specify path so that the url ends with /[inputRef]-[M]/[split]", req, resp);
			return;
		}
		
		String inputRef = idAndM[0];
		int splitCount  = Util.parseNumber(idAndM[1], "/[inputRef]-[M]/[split]", "M",     req, resp);
		int split       = Util.parseNumber(path[2],   "/[inputRef]-[M]/[split]", "split", req, resp);
	
		if (split >= splitCount || split < 0) {
			Util.error("Illegal split "+split+" for splitCount "+splitCount, req, resp);
			return;
		}
		
		// If inputRef can be parsed as an integer, then use chainFSSource
		SplitSource source;
		try {
			Integer.parseInt(inputRef);
			source = chainFSSource;
			
		} catch(Throwable t) {
			source = hugeFileFSSource;
		}
		
		source.retrieveWikiSplit(inputRef, splitCount, split, req, resp);
	}

}
