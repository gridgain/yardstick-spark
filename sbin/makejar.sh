#!/bin/bash -x
curdir=$(pwd)
jdir=/tmp/ujar
cd /tmp
rm -rf ujar
mkdir -p $jdir
cd $curdir

JARS=$(echo "./libs/yardstick-spark-0.0.1.jar
./repo/ignite-yardstick-0.1.0.jar
./repo-aws/ignite-aws-1.4.1-SNAPSHOT.jar
./repo-core/ignite-core-1.4.1-SNAPSHOT.jar
./repo-indexing/ignite-indexing-1.4.1-SNAPSHOT.jar
./repo-log4j/ignite-log4j-1.4.1-SNAPSHOT.jar
./repo-scalar/ignite-scalar-1.4.1-SNAPSHOT.jar
./repo-spark/ignite-spark-1.4.1-SNAPSHOT.jar
./repo1/jfreechart2-2.0-pre1-perfupdate-sources.jar
./repo1/jfreechart2-2.0-pre1-perfupdate.jar
" | tr '\n' ' ')
for jar in $JARS; do cp $jar $jdir; done

cd $jdir
for x in *.jar; do jar -xvf $x; done 
rm *.jar
jar -cvf $curdir/target/yardstick-spark-uber-0.0.1.jar .
cd $curdir


