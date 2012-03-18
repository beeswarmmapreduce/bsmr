How to use the FS:

GET
^^^
http://localhost:8080/fs/splits/1-50/12

(Split #13 of 50 (0..49) from the results of Job #1)

From SplitServlet.java:

A servlet which serves two different styles of input files to BSMR workers. Sources can be
either raw UTF8 files or JSON dicts saved in the format used by BucketServlet. Workers need
to address the splits /[inputRef]-[M]/[split] where inputRef is either an integer for the JSON
dicts from previous runs or a non-integer for UTF-8 files. M is the number of splits in this job
and split is the number of split the worker wants (0..M-1).

If the inputRef is an integer, replies will be JSON dicts otherwise the output is RAW UTF-8 data.
For efficiency reasons, the servlet does not parse the JSON dicts on disk as JSON. Instead if relies
on the file containing the number of keys on the first line. Remaining lines should only contain one
key in its' own dict. For example:

<pre>
[ 4           
,{"scrooge":"1"}
,{"mickey":"4321"}
,{"pluto":"isadog"}
,{"donald":"12345"}
]
</pre>

The values for keys can be of any legal JSON format. The only constraint is that the complete dict 
and the surrounding {} brackets have to fit into one line. 
 
If inputRef is a String, the FS tries to open a file with that name (the directory is parameterized
in web.xml) and a .gz postfix. The file MUST be in UTF-8. For example /foo.xml-10/0 means the first 
split of ten from foo.xml.gz. The file size is calculated and each split will roughly consist of
ceil(size/number_of_splits) bytes of characters. Boundaries between splits will always be on newlines.
So the size of splits will fluctuate depending on how often newlines appear in the original data. 
 
Note that due to the size of the enwiki.xml.gz, the size of that file is hard-coded.
 
 

POST
^^^^
Do a HTTP POST on URL (Partition #3 for Job #1 which has 5 partitions (0..4)):
http://localhost:8080/fs/partitions/1-5/2

Example (raw) HTTP message.
This can be tested e.g. via 'telnet localhost 8080', then paste the POST example message:

POST /fs/partitions/1-5/321 HTTP/1.1
Host: localhost
User-Agent: foo
Content-Type: text/plain
Content-Length: 52

{"key1":"value","key2":value"}

The first partition will start a file in data/1.json which will be of format
[ X
,{"key1":"value"}
,{"key2","value"}
,{"key3","value"}
...
]

Each new partition will add dicts to the file. The last partition will close the file and fill in X with the number of keys. In the example it would be 2

See PartitionServlet.java or the javadoc for more information.


