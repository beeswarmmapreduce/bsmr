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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ChainSplitSourceImpl implements SplitSource 
{
	private static Logger logger = Logger.getLogger(ChainSplitSourceImpl.class.getCanonicalName());


	public static final Pattern PATTERN_KEYCOUNT = Pattern.compile("\\[ *([0-9]+) *");
	public static final Pattern PATTERN_REMOVEDICT = Pattern.compile("[^\\{]*\\{(.*)\\}.*");

	private String loadPath;
	
	public ChainSplitSourceImpl(String path)
	{
		loadPath = path;
	}
	
	@Override
	public void retrieveWikiSplit(Object inputRef, int splitCount, int split,
			HttpServletRequest req, HttpServletResponse resp) throws IOException 
	{
		try {
			
			String fileName = loadPath+"/"+inputRef+".json";
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
		
			String firstLine = br.readLine();
			if (firstLine == null) {
				throw new IOException("File format error");
			}
			
			Matcher m = PATTERN_KEYCOUNT.matcher(firstLine);
			if (!m.matches()) {
				throw new IOException("File format error (2)");
			}
			
			int numKeys = Integer.parseInt(m.group(1));

			
			int keysForThisSplit;
			int startAtKey;
			
			
			if (splitCount > numKeys) {
				
				if (split < numKeys) {
					startAtKey = split;
					keysForThisSplit = 1;
				} else {
					startAtKey = -1;
					keysForThisSplit = 0;
				}
			} else {
				int keysPerSplit = (int)Math.ceil( (double)numKeys / (double)splitCount );
				
				
				//int keysPerSplit = numKeys / (splitCount -1);
				
				startAtKey = keysPerSplit * split;
				
				if (split == splitCount -1) {
					keysForThisSplit = numKeys - startAtKey;
				} else {
					keysForThisSplit = keysPerSplit;
				}
				
			}
			
			int i = 0;
			while (i < startAtKey) {
				br.readLine(); // Throw out the results
				i++;
			}
			
			resp.setCharacterEncoding("UTF-8");
			resp.setContentType("text/plain");
			
			PrintWriter out = resp.getWriter();
			
			out.print("{");

			for (i = 0; i < keysForThisSplit; i++) {
				String s = br.readLine();
				
				m = PATTERN_REMOVEDICT.matcher(s);
				if (!m.matches()) {
					throw new IOException("Line '"+s+"' does not match the regular expression to remove the {} brackets!");
				}
				
				if (i > 0) {
					out.print(',');
				}
	
				out.print(m.group(1));
				out.print('\n');
			}
			out.println("}");

			
			out.flush();
			out.close();
			
		} catch(IOException ie) {
			logger.log(Level.SEVERE, "Could not serve split "+inputRef+", "+splitCount+", "+split, ie);
			Util.error("Error loading split", req, resp);
			return;
		} catch(NumberFormatException nfe) {
			logger.log(Level.SEVERE, "File format contains a non-number number field!", nfe);
			Util.error("File format error (3)", req, resp);
			return;
		}

	}

}
