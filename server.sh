#!/bin/bash

cd $(dirname $0)/master

mvn install
mvn exec:java -Dexec.classpathScope="test" -Dexec.mainClass="StartJetty"

