#!/usr/bin/env bash
SPARK_HOME=$1
HADOOP_VERSION=$2
AWS_SDK_VERSION=$3

mkdir $SPARK_HOME/conf
echo "SPARK_LOCAL_IP=127.0.0.1" > $SPARK_HOME/conf/spark-env.sh
echo "JAVA_HOME=/usr/lib/jvm/$(ls /usr/lib/jvm |grep java)/jre" >> $SPARK_HOME/conf/spark-env.sh

wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars/
wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -P ${SPARK_HOME}/jars/