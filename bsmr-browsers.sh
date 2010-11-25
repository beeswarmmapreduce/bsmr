#!/bin/bash

FF=$1
PAGE=$2
NUM=$3

if [ "$NUM" = "" ]; then
	echo usage: bsmr-browsers.sh path-to-firefox path-to-page number-of-instances
	exit
fi


X=1
while [ $[ $X <= $NUM ] = 1 ]; do
	$FF -no-remote -P 'BSMR#'$X $PAGE &
	X=$[$X+1]
done

wait

