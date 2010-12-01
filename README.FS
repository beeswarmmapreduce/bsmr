How to use the FS:

GET
^^^
http://localhost:8080/fs/splits/1/12

(Split #12 of Job #1, the job doesn't actually affect anything yet)

It will read some number of bytes (can't remember) from data.txt.gz and hand them over.
Uncompression is done at the server, don't worry about that.


POST
^^^^
Do a HTTP POST on URL (Partition #321, Job #1):
http://localhost:8080/fs/partitions/1/321

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

