cat <<-"EOF" >> ~/.bash_profile
export SCALA_HOME=/root/scala-2.11.2
export PATH=$SCALA_HOME/bin:$PATH
export SPARK_HOME=/root/spark-1.4.0
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
EOF

cat <<-"EOF" > ~/run_after_login.sh
export SCALA_HOME=/root/scala-2.11.2
export JAVA_HOME=/usr/lib/jvm/java-1.7.0
export PATH=$SCALA_HOME/bin:$PATH
export PS1="\u@\h \W]\$ "
unset HISTFILESIZE
HISTSIZE=30000
PROMPT_COMMAND="history -a"
export HISTSIZE PROMPT_COMMAND
shopt -s histappend
set -o vi
alias ll='ls -lrta'
export SPARK_HOME=/root/spark-1.4.0
export YARD_SPARK=/root/yardstick-spark
export SL="$(cat $SPARK_HOME/conf/slaves | tr '\n' ' ')"
sshall() { for h in $SL; do echo $h; ssh $h "$1"; done ; }
rsyncall() { for h in $SL; do echo $h; rsync -auv $1 $h: ; done ; }
alias b='vi ~/.bashrc; source ~/.bashrc'
slave1() { echo "$SL)" | cut -d' ' -f1 ; }
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
sparkpi() { spark-submit --class org.apache.spark.examples.SparkPi $SPARK_HOME/lib/*example*.jar ; }
hist() { history | tail -n $1 ; }
zinc() { /root/zinc-0.3.7/bin/zinc -scala-home /root/scala-2.11.2 -nailed -start ; }
export MASTER="spark://$(curl -s http://169.254.169.254/latest/meta-data/public-hostname):7077"
echo "MASTER IS $MASTER"
afterlogin() { source ~/run-after-login.sh; } 
EOF
chmod +x ~/run_after_login.sh
cp ~/run_after_login.sh ~/.bashrc
source ~/.bashrc
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
source ~/.bashrc
zinc  # launch zinc server
cd $SPARK_HOME
mvn -Pyarn -Phive -Phadoop-2.4 -Dscala-2.11 -DskipTests clean package

cd /root 
git clone https://github.com/ThirdEyeCSS/yardstick-spark
cd /root/yardstick-spark/
git checkout coresql
git fetch origin coresql
git rebase origin/coresql
mvn clean package
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
rsyncall  ~/run-after-login.sh
#spark-submit --master $(MASTER) --class org.yardstickframework.spark.SparkCoreRDDBenchmark ./target/yardstick-spark-uber-0.0.1.jar -CORE_CONFIG_FILE=config/coreTests.yml -cfg file:///root/yardstick-spark/config/hosts.xml -nn 1 -v -b 1 -w 60 -d 10 -t 1  -sm PRIMARY_SYNC -dn SparkCoreRDDBenchmark -cn tx -sn SparkNode


