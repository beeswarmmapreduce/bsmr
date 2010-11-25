#!/bin/bash

CP=""
for FILE in $(find master/jetty-*/lib/*); do
	CP=$CP":"$FILE
done

if [ "$CP" = "" ]; then
    cd master/
    ./getdeps.sh
    echo '#############################################################'
    echo ' re-run ./compile-server.sh'
    echo '#############################################################'
    exit
fi

DEST=master/bin/

javac -classpath $CP -d $DEST -sourcepath master/src/ $(find master/src/ -name "*.java")

cp master/src/data.txt.gz $DEST

