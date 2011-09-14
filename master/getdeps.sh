#!/bin/sh

VERSION=8.0.1.v20110908


wget http://download.eclipse.org/jetty/$VERSION/dist/jetty-distribution-$VERSION.tar.gz && \
tar xf jetty-distribution-$VERSION.tar.gz && \
rm jetty-distribution-$VERSION.tar.gz
