#!/bin/bash

CP=""
for FILE in $(find master/jetty-*/lib/*); do
	CP=$CP":"$FILE
done

DEST=master/bin/

javac -classpath $CP -d $DEST -sourcepath master/src/ $(find master/src/ -name "*.java")

cp master/src/data.txt.gz $DEST

