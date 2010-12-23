#!/bin/bash

CP="master/bin"
for FILE in $(find master/jetty-*/lib/*) lib/*.jar; do
	CP=$CP":"$FILE
done

java -classpath $CP StartJetty

