echo "source ~/.bashrc" >> ~/.bash_profile
cat <<-"EOF" > ~/.bashrc
export SCALA_HOME=/root/scala-2.11.2
export JAVA_HOME=/usr/lib/jvm/java-1.7.0
export PATH=$SCALA_HOME/bin:$PATH
export PS1="\u@\h \W]\$ "
export HDFS_URL=hdfs://ip-10-144-237-50.ec2.internal:9000
unset HISTFILESIZE
HISTSIZE=30000
PROMPT_COMMAND="history -a"
export HISTSIZE PROMPT_COMMAND
shopt -s histappend
set -o vi
alias ll='ls -lrta'
#export SPARK_HOME=/root/spark
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
EOF
source ~/.bashrc
cd /root
sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
sudo yum install -y apache-maven
mvn --version
git clone https://github.com/ThirdEyeCSS/yardstick-spark
cd yardstick-spark/
git checkout coresql
git fetch origin coresql
git rebase origin/coresql
mvn clean package
rsyncall /root/yardstick-spark
sshall "touch /root/.bashrc"
rsyncall /root/.bashrc
rsyncall /root/.bash_profile

