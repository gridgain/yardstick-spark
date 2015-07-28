#!/bin/bash -x
curdir=$(pwd)
jdir=/tmp/ujar
cd /tmp
rm -rf ujar
mkdir -p $jdir
cd $curdir
cp libs/yardstick-spark-0*.jar $jdir
cp libs/ignite-yardstick-0.1.0.jar $jdir
cd $jdir
for x in *.jar; do jar -xvf $x; done 
rm *.jar
jar -cvf $curdir/target/yardstick-spark-uber-0.0.1.jar .
cd $curdir


