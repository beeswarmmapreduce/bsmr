How to use the FS:

GET
^^^
http://localhost:8080/fs/splits/1-50/12

(Split #13 of 50 (0..49) from the results of Job #1)

From SplitServlet.java:

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
 

POST
^^^^
Do a HTTP POST on URL (Partition #3 for Job #1 which has 5 partitions (0..4)):
http://localhost:8080/fs/partitions/1-5/2

Example (raw) HTTP message.
This can be tested e.g. via 'telnet localhost 8080', then paste the POST example message:

POST /fs/partitions/1/321 HTTP/1.1
Host: localhost
User-Agent: foo
Content-Type: text/plain
Content-Length: 5

12345

This will save a file under the bsmr root directory done/1/321 (it will create the directories 
necessary).

