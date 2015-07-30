#!/bin/bash -x

CONF=${1:-"$IGNITE_HOME/config/spark-aws-config.xml"}
YS_JVM_OPTS=${YS_JVM_OPTS:-"-DIGNITE_QUIET=false -Xms2048m  -Xmx8192m  -XX:MaxPermSize=512m"}
FIXED_JVM_OPTS="-Xloggc:./gc.log -XX:+PrintGCDetails  -verbose:gc  -XX:+UseParNewGC  -XX:+UseConcMarkSweepGC  -XX:+UseTLAB  -XX:NewSize=128m  -XX:MaxNewSize=128m  -XX:MaxTenuringThreshold=0  -XX:SurvivorRatio=1024  -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=60 -DIGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK=true"
export JVM_OPTS="$YS_JVM_OPTS $FIXED_JVM_OPTS"

kill -9 $(ps -ef | egrep IGNITE_PROG_NAME | grep -v grep | awk '{print $2}')

#CMD="export JVM_OPTS='$JVM_OPTS' && $IGNITE_HOME/bin/ignite.sh $CONF"
$IGNITE_HOME/bin/ignite.sh $CONF

#echo "$CMD | tee $IGNITE_HOME/logs/ignite.$(date +%M%D-%H%M).log
#echo "bash -c '$CMD' | tee /tmp/ignite.log" | at now
sleep 3
echo "ignite is running with pid= $(ps -ef | egrep IGNITE_PROG_NAME | grep -v grep | awk '{print $2}')"
#echo "atq: " && atq
#sleep 100000

#!/bin/bash -x

CONF=${1:-"$IGNITE_HOME/config/spark-aws-config.xml"}
YS_JVM_OPTS=${YS_JVM_OPTS:-"-DIGNITE_QUIET=false -Xms2048m  -Xmx8192m  -XX:MaxPermSize=512m"}
FIXED_JVM_OPTS= -Xloggc:./gc.log -XX:+PrintGCDetails  -verbose:gc  -XX:+UseParNewGC  -XX:+UseConcMarkSweepGC  -XX:+UseTLAB  -XX:NewSize=128m  -XX:MaxNewSize=128m  -XX:MaxTenuringThreshold=0  -XX:SurvivorRatio=1024  -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=60 
export JVM_OPTS="$YS_JVM_OPTS $FIXED_JVM_OPTS"

kill -9 $(ps -ef | egrep IGNITE_PROG_NAME | grep -v grep | awk '{print $2}')


CMD="export JVM_OPTS=$JVM_OPTS && $IGNITE_HOME/bin/ignite.sh $CONF"
echo $CMD

$(CMD)

#echo "$CMD | tee $IGNITE_HOME/logs/ignite.$(date +%M%D-%H%M).log
#echo "bash -c '$CMD' | tee /tmp/ignite.log" | at now
sleep 3
echo "ignite is running with pid= $(ps -ef | egrep IGNITE_PROG_NAME | grep -v grep | awk '{print $2}')"
echo "atq: " && atq
sleep 100000
