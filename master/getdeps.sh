#!/bin/sh

VERSION=8.0.0.M2

wget http://eclipse.nordnet.fi/eclipse/jetty/$VERSION/dist/jetty-distribution-$VERSION.tar.bz2 && \
tar xf jetty-distribution-$VERSION.tar.bz2 && \
rm jetty-distribution-$VERSION.tar.bz2

