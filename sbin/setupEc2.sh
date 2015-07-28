cat <<-"EOF" >> ~/.bash_profile
export JAVA_HOME=/usr/lib/jvm/java-1.7.0
export SCALA_HOME=/root/scala-2.11.2
export PATH=$SCALA_HOME/bin:$PATH
export SPARK_HOME=/root/spark-1.4.0
export PATH=.:"$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
export PS1="\u@\h \W]\$ "
unset HISTFILESIZE
HISTSIZE=30000
PROMPT_COMMAND="history -a"
export HISTSIZE PROMPT_COMMAND
shopt -s histappend
set -o vi
alias ll='ls -lrta'
hist() { history | tail -n $1 ; }
EOF

LOGON_SCRIPT=~/logon-script.sh
cat <<-"EOF" > $LOGON_SCRIPT 
export YARD_SPARK=/root/yardstick-spark
export SL="$(cat $SPARK_HOME/conf/slaves | tr '\n' ' ')"
sshall() { for h in $SL; do echo $h; ssh $h "$1"; done ; }
rsyncall() { for h in $SL; do echo $h; rsync -auv $1 $h: ; done ; }
alias b='vi $LOGON_SCRIPT; source $LOGON_SCRIPT'
slave1() { echo "$SL" | cut -d' ' -f1 ; }
sparkpi() { spark-submit --master $MASTER --class org.apache.spark.examples.SparkPi $SPARK_HOME/examples/target/scala-2.11/*example*.jar ; }
zinc() { /root/zinc-0.3.7/bin/zinc -scala-home /root/scala-2.11.2 -nailed -start ; }
export MASTER="spark://$(hostname):7077"
export EXTERNALIP="spark://$(curl -s http://169.254.169.254/latest/meta-data/public-hostname):7077"
echo "MASTER IS $MASTER"
logon() { source $LOGON_SCRIPT; } 
EOF
sed -i $LOGON_SCRIPT -e "s/\$LOGON_SCRIPT/$LOGON_SCRIPT/g"
chmod +x $LOGON_SCRIPT
source $LOGON_SCRIPT
cd /root
sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
sudo yum install -y apache-maven
mvn --version
wget https://github.com/apache/spark/archive/v1.4.0.tar.gz
tar -xvf v1.4.0.tar.gz
wget http://downloads.typesafe.com/scala/2.11.2/scala-2.11.2.tgz
tar -xvf scala-2.11.2.tgz
wget http://downloads.typesafe.com/zinc/0.3.7/zinc-0.3.7.tgz
tar -xvf zinc-0.3.7.tgz
cp -p /root/spark/conf/slaves /root/spark-1.4.0/conf/
source $LOGON_SCRIPT  # need to do this after copying conf/slaves to get updated slaves list
zinc  # launch zinc server
cd $SPARK_HOME
dev/change-version-to-2.11.sh
sed -i pom.xml -e "s/2\.10\.4/2\.11\.2/g"
mvn -Pyarn -Phive -Phadoop-2.4 -Dscala-2.11 -DskipTests -Dmaven.javadoc.skip=true clean package
$SPARK_HOME/sbin/stop-all.sh
$SPARK_HOME/sbin/start-all.sh

cd /root 
git clone https://github.com/ThirdEyeCSS/yardstick-spark
cd /root/yardstick-spark/
git checkout coresql
git fetch origin coresql
git rebase origin/coresql
mvn clean package
sbin/makejar.sh
rsyncall /root/yardstick-spark

cd /root
wget http://downloads.sourceforge.net/project/s3tools/s3cmd/1.5.0-rc1/s3cmd-1.5.0-rc1.tar.gz
tar zxf s3cmd-1.5.0-rc1.tar.gz
cd s3cmd-1.5.0-rc1
sudo python setup.py install
s3cmd --configure
s3cmd --version
export PATH=$PATH:$(pwd)

rsyncall /root/scala-2.11.2 
sshall "mkdir /root/spark-1.4.0"
rsyncall /root/spark-1.4.0
rsyncall ~/.bash_profile
rsyncall $LOGON_SCRIPT 
#spark-submit --master $MASTER --class org.yardstickframework.spark.SparkCoreRDDBenchmark ./target/yardstick-spark-uber-0.0.1.jar -CORE_CONFIG_FILE=config/coreTests.yml -cfg file:///root/yardstick-spark/config/hosts.xml -nn 1 -v -b 1 -w 60 -d 10 -t 1  -sm PRIMARY_SYNC -dn SparkCoreRDDBenchmark -cn tx -sn SparkNode


