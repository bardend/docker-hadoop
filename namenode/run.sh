#!/bin/bash
set -e
if [ -z "$CLUSTER_NAME" ]; then
    echo "We have created a bridge network to detect resolution dns, so set the CLUSTER_NAME :)"
    exit 2
fi

NAME_DIR="${HDFS_CONF_dfs_namenode_name_dir:-/hadoop/dfs/name}"

rm -rf $NAME_DIR/lost+found

if [ "$(ls -A $NAME_DIR)" == "" ]; then 
    echo "We need to format the namenode directory: $NAME_DIR"
    $HADOOP_HOME/bin/hdfs namenode -format $CLUSTER_NAME
fi

$HADOOP_HOME/bin/hdfs namenode &
sleep infinity
