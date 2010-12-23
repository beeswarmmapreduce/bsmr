package fi.helsinki.cs.bsmr.fs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class HugeUTF8SplitSourceImpl implements SplitSource 
{
	private String loadPath;
	
	private static Map<String, Long> fileSizeCache;
	
	public HugeUTF8SplitSourceImpl(String path)
	{
		loadPath = path;
	}

	static {
		fileSizeCache = new HashMap<String, Long>();
	
		// TODO: this should be read somehow, but it's slightly difficult. "gzip -l xx.gz" does work though..
		fileSizeCache.put("enwiki.xml", 805506686l);
		
	}
	
	@Override
	public void retrieveWikiSplit(Object inputRef, int splitCount, int split,
			HttpServletRequest req, HttpServletResponse resp) throws IOException
	{
		String fileName = loadPath+"/"+inputRef+".gz"; 
		
		long fileSize;
		
		if ("enwiki.xml".equals(inputRef)) {
			// TODO: see static {} 
			fileSize = fileSizeCache.get("enwiki.xml"); 
		} else {
			fileSize = calculateUncompressedFileSize(fileName);

		}
		
		long splitSize = (long)Math.ceil((double)fileSize/(double)splitCount);
		
		long skip = split * splitSize;
		
		InputStream is = new FileInputStream(fileName);
		if (is == null) {
			Util.error("Could not open '"+fileName+"'", req, resp);
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
				Util.error("not enough data in input file for split "+split, req, resp);
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
		OutputStream os = resp.getOutputStream();
		
		while (i< splitSize) {
			s = br.readLine();
			
			if (s == null) break; // EOF
			
			byte [] asBytes = s.getBytes(utf8);
			
			os.write(asBytes);
			os.write('\n');
			
			i += asBytes.length;
			i ++; // newline!
		}
		os.flush();
		os.close();
	}

	private static long calculateUncompressedFileSize(String fileName) throws IOException 
	{
		synchronized (fileSizeCache) {
			Long ret = fileSizeCache.get(fileName);
			if (ret != null) {
				return ret;
			}
		
			GZIPInputStream is = new GZIPInputStream(new FileInputStream(fileName));
			
			long i;
			long c = 0;
			
			while ( (i = is.skip(1024*1024)) > 0) { c+=i; }
			
			fileSizeCache.put(fileName, c);
			return c;
		}
	}

}
