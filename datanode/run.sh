#!/bin/bash
<<'COMMENT'
All env variables are setting in the base and modify in the docker-compose
COMMENT
set -e
#exec $HADOOP_HOME/bin/hdfs datanode
$HADOOP_HOME/bin/hdfs datanode &
sleep infinity