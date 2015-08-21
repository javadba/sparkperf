#!/bin/bash
cat <<-"EOF" > ~/.bash_profile
export JAVA_HOME=/usr/lib/jvm/java-1.7.0
export SCALA_HOME=/mnt/scala-2.11.2
export PATH=$SCALA_HOME/bin:$PATH
export SPARK_HOME=/mnt/spark-1.4.1
export PATH=.:"$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
export IGNITE_HOME=/mnt/ignite
export PS1="\u@\h \W]\$ "
unset HISTFILESIZE
HISTSIZE=30000
PROMPT_COMMAND="history -a"
export HISTSIZE PROMPT_COMMAND
shopt -s histappend
set -o vi
alias ll='ls -lrta'
hist() { history | tail -n $1 ; }
isigup() { ps -ef | egrep IGNITE_PROG_NAME | grep -v grep | awk '{print $2}' ; }
export YS=/root/yardstick-spark
EOF

export LOGON_SCRIPT=~/logon-script.sh
cat <<-"EOF" > $LOGON_SCRIPT 
export LOGON_SCRIPT=~/logon-script.sh
export YARD_SPARK=/root/yardstick-spark
export SL="$(cat $SPARK_HOME/conf/slaves | tr '\n' ' ')"
sshall() { for h in $SL; do echo $h; ssh $h "$1"; done ; }
rsyncall() { dest=${2:-"$1"}; for h in $SL; do echo $h; rsync -auv $1 $h:$dest ; done ; }
alias b='vi $LOGON_SCRIPT; source $LOGON_SCRIPT'
slave1() { echo "$SL" | cut -d' ' -f1 ; }
sparkpi() { spark-submit --master $EXTERNALIP --class org.apache.spark.examples.SparkPi $SPARK_HOME/examples/target/scala-2.11/*example*.jar ; }
zinc() { /mnt/zinc-0.3.7/bin/zinc -scala-home /mnt/scala-2.11.2 -nailed -start ; }
export MASTER="spark://$(hostname):7077"
export EXTERNALIP="spark://$(curl -s http://169.254.169.254/latest/meta-data/public-hostname):7077"
echo "MASTER IS $MASTER"
logon() { source $LOGON_SCRIPT; } 
EOF
sed -i $LOGON_SCRIPT -e "s/\$LOGON_SCRIPT/$LOGON_SCRIPT/g"
chmod +x $LOGON_SCRIPT
source $LOGON_SCRIPT
source ~/.bash_profile
cd /mnt
sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
sudo yum install -y apache-maven
mvn --version
cd /mnt
#wget http://mirrors.ibiblio.org/apache//incubator/ignite/1.3.0/apache-ignite-1.3.0-incubating-src.zip
#unzip  apache-ignite-1.3.0-incubating-src.zip
#ln -s /mnt/apache-ignite-1.3.0-incubating-src /mnt/ignite
#cd /mnt/ignite
#mvn clean package -DskipTests -Dmaven.javadoc.skip=true -Prelease,lgpl
#echo "export IGNITE_HOME=/mnt/ignite" >> ~/.bash_profile
#export IGNITE_HOME=/mnt/ignite
#sshall "mkdir -p $IGNITE_HOME"
ln -s $YARD_SPARK/config/spark-aws-config.xml /mnt/ignite/config/spark-aws-config.xml 
#rsyncall $IGNITE_HOME/

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
dev/change-scala-version.sh 2.11
# sed -i pom.xml -e "s/2\.10\.4/2\.11\.2/g"
mvn -Pyarn -Phive -Phadoop-2.4 -Dscala-2.11 -DskipTests -Dmaven.javadoc.skip=true clean package

cd /root 
git clone https://github.com/ThirdEyeCSS/yardstick-spark
cd /root/yardstick-spark/
git checkout coresql
git fetch origin coresql
git rebase origin/coresql
mvn -DskipTests=true -Dmaven.javadoc.skip=true clean package
mvn -DskipTests=true -Dmaven.javadoc.skip=true assembly:single 
sbin/makejar.sh
rsyncall /root/yardstick-spark/

cd /root
wget http://downloads.sourceforge.net/project/s3tools/s3cmd/1.5.0-rc1/s3cmd-1.5.0-rc1.tar.gz
tar zxf s3cmd-1.5.0-rc1.tar.gz
cd s3cmd-1.5.0-rc1
sudo python setup.py install
s3cmd --configure
s3cmd --version
export PATH=$PATH:$(pwd)

sshall "mkdir $SCALA_HOME"
rsyncall /mnt/scala-2.11.2/
sshall "mkdir $SPARK_HOME"
rsyncall $SPARK_HOME/
rsyncall ~/.bash_profile
rsyncall $LOGON_SCRIPT 
$SPARK_HOME/sbin/stop-all.sh
$SPARK_HOME/sbin/start-all.sh
