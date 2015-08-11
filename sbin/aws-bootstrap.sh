source ~/logon-script.sh
source ~/.bash_profile
cd /mnt
sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
sudo yum install -y apache-maven
mvn --version
cd /mnt
wget https://github.com/apache/spark/archive/v1.4.1.tar.gz
tar -xvf v1.4.1.tar.gz
wget http://downloads.typesafe.com/scala/2.11.2/scala-2.11.2.tgz
tar -xvf scala-2.11.2.tgz
wget http://downloads.typesafe.com/zinc/0.3.7/zinc-0.3.7.tgz
tar -xvf zinc-0.3.7.tgz
cp -p /root/spark/conf/slaves $SPARK_HOME/conf/
source $LOGON_SCRIPT  # need to do this after copying conf/slaves to get updated slaves list
zinc  # launch zinc server
cd $SPARK_HOME
dev/change-version-to-2.11.sh
sed -i pom.xml -e "s/2\.10\.4/2\.11\.2/g"
mvn -Pyarn -Phive -Phadoop-2.4 -Dscala-2.11 -DskipTests -Dmaven.javadoc.skip=true clean package

sshall "mkdir $SCALA_HOME"
rsyncall /mnt/scala-2.11.2/
sshall "mkdir $SPARK_HOME"
rsyncall $SPARK_HOME/
ln -s /root/spark/conf/slaves $SPARK_HOME/conf/
ln -s /root/spark/conf/core-site.xml $SPARK_HOME/conf/

cp -p /root/spark/conf/spark-env.sh $SPARK_HOME/conf

echo "
export SPARK_WORKER_MEMORY=11500M
export SPARK_WORKER_CORES=8
export SPARK_DRIVER_MEMORY=11500M
export SPARK_DRIVER_CORES=8
" >>   $SPARK_HOME/conf/spark-env.sh

