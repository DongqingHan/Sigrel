#!/bin/bash 

MAIN_CLASS="JobLoop"

if [ -z "$HADOOP_HOME" ]; then
    export HADOOP=`which hadoop`
else
    export HADOOP="$HADOOP_HOME/bin/hadoop"
fi

basedir=`dirname "${BASH_SOURCE-$0}"`
jardir="/var/lib/hadoop-hdfs/handongqing/libjars"
libdir="/var/lib/hadoop-hdfs/handongqing/libjars"
#JAR=$(ls $jardir/anti-fraud*.jar)
JAR=$(ls $jardir/sigrel*.jar)
dependencies=$(ls $libdir/*.jar | tr '\n' :)
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$dependencies
export LIBJARS=`echo ${dependencies} | sed s/:/,/g`
export JAVA_HOME=/usr/java/jdk

echo "run command : $HADOOP jar ${JAR} $MAIN_CLASS -libjars ${LIBJARS} input_path output_path "

$HADOOP jar ${JAR} $MAIN_CLASS -libjars ${LIBJARS} /user/handongqing/tiny_graph.txt /user/handongqing/loop_test2

if [ $? -ne 0 ]
then
    echo "run mapreduce failed !"
    exit 255
fi

 
