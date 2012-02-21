#!/bin/bash
export CLASSPATH=`pwd`
for f in *.jar; do export CLASSPATH=$CLASSPATH:`pwd`/$f; done
