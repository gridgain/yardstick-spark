#!/bin/bash -x
curdir=$(pwd)
jdir=/tmp/ujar
cd /tmp
rm -rf ujar
mkdir -p $jdir
cd $curdir

JARS=$(echo "./libs/yardstick-spark-0.0.1.jar
./repo/ignite-yardstick-0.1.0.jar
./repo-aws/ignite-aws-1.5.0-SNAPSHOT.jar
./repo-core/ignite-core-1.5.0-SNAPSHOT.jar
./repo-indexing/ignite-indexing-1.5.0-SNAPSHOT.jar
./repo-log4j/ignite-log4j-1.5.0-SNAPSHOT.jar
./repo-scalar/ignite-scalar-1.5.0-SNAPSHOT.jar
./repo-spring/ignite-spring-1.5.0-SNAPSHOT.jar
./repo-spark/ignite-spark-1.5.0-SNAPSHOT.jar
./libs/aws-java-sdk-1.3.21.1.jar
" | tr '\n' ' ')
for jar in $JARS; do cp $jar $jdir; done
for jar in libs/*; do echo $jar; cp $jar $jdir; done
for jar in libs2/*; do echo $jar; cp $jar $jdir; done
for jar in install_libs/*; do echo $jar; cp $jar $jdir; done
cd $jdir
for x in *.jar; do jar -xvf $x; done
rm *.jar
cd META-INF
rm -rf *.SF
rm -rf *.DSA
rm -rf *.RSA
rm -rf ECLIPSE*
rm -rf license/*
cd ..
jar -cvf $curdir/target/yardstick-spark-uber-0.0.1.jar .
cd $curdir
