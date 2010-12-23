import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;


public class SimpleWordCount
{
	private Map<String, MutableInteger> counts;
	private BufferedReader br;
	private String regex;
	
	public SimpleWordCount(InputStream is, String regex)
	{
		this.regex = regex;
		this.counts = new HashMap<String, MutableInteger>();
		this.br = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
	}
	
	public void count() throws IOException
	{
		String s;
		
		while ( (s = br.readLine()) != null) {
			String [] tmp = s.split(regex);
		
			for (String word : tmp) {
				MutableInteger mi = counts.get(word);
				if (mi == null) {
					mi = new MutableInteger();
					counts.put(word, mi);
				}
				mi.n++;
			}
			
		}
	}
	
	public class MutableInteger
	{
		public int n;
		
		public MutableInteger()
		{
			n = 0;
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException
	{
		String file = "data/data.txt.gz";
		FileInputStream fis = new FileInputStream(file);
		GZIPInputStream gzis = new GZIPInputStream(fis);
		SimpleWordCount swc = new SimpleWordCount(gzis, "\\W+");
		
		System.out.println("Solving incredibly difficult differential equations..");
		swc.count();
		
		System.out.println("Done! Write the name of the key to check word count");
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		String key;
		System.out.print("> "); System.out.flush();
		while ( (key = br.readLine()) != null) {
			
			MutableInteger mi = swc.counts.get(key);
			System.out.println("'"+key+"': "+(mi != null ? mi.n : "N/A"));
			
			System.out.print("> "); System.out.flush();
		}

	}

}
